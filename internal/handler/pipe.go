package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

// PipeHandler manages StarRocks PIPEs (continuous, incremental loads from object
// storage). A PIPE watches a path and auto-ingests new files (dedup by name+digest),
// so date-partitioned buckets are handled with a wildcard — no scheduler or date math.
type PipeHandler struct {
	pools *pool.Manager
}

func NewPipeHandler(pools *pool.Manager) *PipeHandler {
	return &PipeHandler{pools: pools}
}

// List returns all pipes from information_schema.pipes.
func (h *PipeHandler) List(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := "SELECT" +
		" DATABASE_NAME as DbName," +
		" PIPE_ID as PipeId," +
		" PIPE_NAME as PipeName," +
		" STATE as State," +
		" TABLE_NAME as TableName," +
		" LOAD_STATUS as LoadStatus," +
		" PROPERTIES as Properties," +
		" LAST_ERROR as LastError," +
		" CREATED_TIME as CreatedTime" +
		" FROM information_schema.pipes"
	if dbName := r.URL.Query().Get("db"); dbName != "" {
		query += " WHERE DATABASE_NAME = '" + strings.ReplaceAll(dbName, "'", "''") + "'"
	}
	query += " ORDER BY CREATED_TIME DESC"

	results, err := queryToMaps(db, r, query)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list pipes: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// Files returns the per-file load status of a pipe from information_schema.pipe_files.
func (h *PipeHandler) Files(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	pipe := strings.TrimSpace(r.URL.Query().Get("pipe"))
	if pipe == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "pipe is required")
		return
	}
	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "100"
	}

	query := "SELECT" +
		" DATABASE_NAME as DbName," +
		" PIPE_NAME as PipeName," +
		" FILE_NAME as FileName," +
		" FILE_SIZE as FileSize," +
		" LOAD_STATE as LoadState," +
		" ERROR_MSG as ErrorMsg," +
		" STAGED_TIME as StagedTime," +
		" START_LOAD_TIME as StartLoadTime," +
		" FINISH_LOAD_TIME as FinishLoadTime" +
		" FROM information_schema.pipe_files" +
		" WHERE PIPE_NAME = '" + strings.ReplaceAll(pipe, "'", "''") + "'"
	if dbName := r.URL.Query().Get("db"); dbName != "" {
		query += " AND DATABASE_NAME = '" + strings.ReplaceAll(dbName, "'", "''") + "'"
	}
	query += " ORDER BY STAGED_TIME DESC LIMIT " + limit

	results, err := queryToMaps(db, r, query)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list pipe files: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// Create creates a new pipe. The frontend builds the full CREATE PIPE statement.
func (h *PipeHandler) Create(w http.ResponseWriter, r *http.Request) {
	db, ok := h.writableDB(w, r)
	if !ok {
		return
	}

	var req struct {
		SQL      string `json:"sql"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sqlStmt := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(sqlStmt)
	if !strings.HasPrefix(upper, "CREATE PIPE") && !strings.HasPrefix(upper, "CREATE OR REPLACE PIPE") {
		stream.WriteJSONError(w, http.StatusForbidden, "only CREATE PIPE statements are allowed")
		return
	}

	if err := execWithDatabase(r.Context(), db, req.Database, sqlStmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Pipe created successfully",
	})
}

// Alter suspends or resumes a pipe. SUSPEND/RESUME are not database-qualified, so we
// set the database context on a dedicated connection before issuing the ALTER.
func (h *PipeHandler) Alter(w http.ResponseWriter, r *http.Request) {
	db, ok := h.writableDB(w, r)
	if !ok {
		return
	}

	var req struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		Action   string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	action := strings.ToUpper(strings.TrimSpace(req.Action))
	if action != "SUSPEND" && action != "RESUME" {
		stream.WriteJSONError(w, http.StatusBadRequest, "action must be suspend or resume")
		return
	}
	dbName := stripBackticks(req.Database)
	name := stripBackticks(req.Name)
	if dbName == "" || name == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "database and name are required")
		return
	}

	conn, err := db.Conn(r.Context())
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer conn.Close()

	if _, err := conn.ExecContext(r.Context(), "USE `"+dbName+"`"); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if _, err := conn.ExecContext(r.Context(), "ALTER PIPE `"+name+"` "+action); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Pipe " + strings.ToLower(action) + "d successfully",
	})
}

// pipeExecUseDB runs an ALTER PIPE statement on a dedicated connection with the
// database selected first (SUSPEND/RESUME/SET/RETRY are not reliably db-qualified).
func pipeExecUseDB(ctx context.Context, db *sql.DB, dbName, stmt string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "USE `"+dbName+"`"); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, stmt)
	return err
}

// SetProperty changes a pipe's properties (e.g. AUTO_INGEST, POLL_INTERVAL).
func (h *PipeHandler) SetProperty(w http.ResponseWriter, r *http.Request) {
	db, ok := h.writableDB(w, r)
	if !ok {
		return
	}
	var req struct {
		Database   string            `json:"database"`
		Name       string            `json:"name"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dbName := stripBackticks(req.Database)
	name := stripBackticks(req.Name)
	if dbName == "" || name == "" || len(req.Properties) == 0 {
		stream.WriteJSONError(w, http.StatusBadRequest, "database, name and properties are required")
		return
	}
	parts := make([]string, 0, len(req.Properties))
	for k, v := range req.Properties {
		parts = append(parts, `"`+stripBackticks(k)+`" = "`+strings.ReplaceAll(v, `"`, "")+`"`)
	}
	stmt := "ALTER PIPE `" + name + "` SET PROPERTY (" + strings.Join(parts, ", ") + ")"
	if err := pipeExecUseDB(r.Context(), db, dbName, stmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{"success": true, "message": "Pipe properties updated"})
}

// Retry re-loads errored files of a pipe: RETRY ALL, or RETRY FILE '<path>'.
func (h *PipeHandler) Retry(w http.ResponseWriter, r *http.Request) {
	db, ok := h.writableDB(w, r)
	if !ok {
		return
	}
	var req struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		File     string `json:"file"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dbName := stripBackticks(req.Database)
	name := stripBackticks(req.Name)
	if dbName == "" || name == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "database and name are required")
		return
	}
	var stmt string
	if file := strings.TrimSpace(req.File); file != "" {
		stmt = "ALTER PIPE `" + name + "` RETRY FILE '" + strings.ReplaceAll(file, "'", "") + "'"
	} else {
		stmt = "ALTER PIPE `" + name + "` RETRY ALL"
	}
	if err := pipeExecUseDB(r.Context(), db, dbName, stmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{"success": true, "message": "Retry triggered"})
}

// Drop deletes a pipe by name.
func (h *PipeHandler) Drop(w http.ResponseWriter, r *http.Request) {
	db, ok := h.writableDB(w, r)
	if !ok {
		return
	}

	var req struct {
		Database string `json:"database"`
		Name     string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dbName := stripBackticks(req.Database)
	name := stripBackticks(req.Name)
	if dbName == "" || name == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "database and name are required")
		return
	}

	sql := "DROP PIPE IF EXISTS `" + dbName + "`.`" + name + "`"
	if _, err := db.ExecContext(r.Context(), sql); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Pipe dropped successfully",
	})
}

// writableDB resolves the env pool and enforces admin write access (read-only guard).
func (h *PipeHandler) writableDB(w http.ResponseWriter, r *http.Request) (*sql.DB, bool) {
	envName := middleware.EnvFromContext(r.Context())
	e, exists := h.pools.GetEnv(envName)
	if !exists {
		stream.WriteJSONError(w, http.StatusInternalServerError, "unknown environment")
		return nil, false
	}
	if e.ReadOnly {
		stream.WriteJSONError(w, http.StatusForbidden, "this environment is read-only")
		return nil, false
	}
	pooledDB, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return nil, false
	}
	return pooledDB, true
}

func stripBackticks(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), "`", "")
}
