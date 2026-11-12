package handler

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

// MaterializedViewHandler manages StarRocks async materialized views — list with
// refresh state/staleness, manual REFRESH, and DROP. MVs pre-compute query results
// and StarRocks keeps them fresh on a schedule; this is native (information_schema
// .materialized_views + REFRESH/DROP MATERIALIZED VIEW), we only wrap it.
type MaterializedViewHandler struct {
	pools *pool.Manager
}

func NewMaterializedViewHandler(pools *pool.Manager) *MaterializedViewHandler {
	return &MaterializedViewHandler{pools: pools}
}

// List returns all materialized views with their refresh state.
func (h *MaterializedViewHandler) List(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := "SELECT" +
		" TABLE_SCHEMA as DbName," +
		" TABLE_NAME as MvName," +
		" REFRESH_TYPE as RefreshType," +
		" IS_ACTIVE as IsActive," +
		" INACTIVE_REASON as InactiveReason," +
		" LAST_REFRESH_STATE as LastRefreshState," +
		" LAST_REFRESH_START_TIME as LastRefreshStart," +
		" LAST_REFRESH_FINISHED_TIME as LastRefreshFinished," +
		" LAST_REFRESH_ERROR_MESSAGE as LastRefreshError," +
		" TABLE_ROWS as TableRows," +
		" MATERIALIZED_VIEW_DEFINITION as Definition" +
		" FROM information_schema.materialized_views"
	if dbName := r.URL.Query().Get("db"); dbName != "" {
		query += " WHERE TABLE_SCHEMA = '" + strings.ReplaceAll(dbName, "'", "''") + "'"
	}
	query += " ORDER BY TABLE_SCHEMA, TABLE_NAME"

	results, err := queryToMaps(db, r, query)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list materialized views: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// Create creates a materialized view. The frontend builds the full statement.
func (h *MaterializedViewHandler) Create(w http.ResponseWriter, r *http.Request) {
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
	if !strings.HasPrefix(strings.ToUpper(sqlStmt), "CREATE MATERIALIZED VIEW") {
		stream.WriteJSONError(w, http.StatusForbidden, "only CREATE MATERIALIZED VIEW statements are allowed")
		return
	}
	if err := execWithDatabase(r.Context(), db, req.Database, sqlStmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Materialized view created",
	})
}

// Refresh triggers an async refresh of a materialized view.
func (h *MaterializedViewHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	db, ok := h.writableDB(w, r)
	if !ok {
		return
	}

	var req struct {
		Database string `json:"database"`
		Name     string `json:"name"`
		Force    bool   `json:"force"`
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

	sqlStmt := "REFRESH MATERIALIZED VIEW `" + dbName + "`.`" + name + "`"
	if req.Force {
		sqlStmt += " WITH SYNC MODE FORCE"
	}
	if _, err := db.ExecContext(r.Context(), sqlStmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Refresh triggered",
	})
}

// Drop deletes a materialized view.
func (h *MaterializedViewHandler) Drop(w http.ResponseWriter, r *http.Request) {
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

	if _, err := db.ExecContext(r.Context(), "DROP MATERIALIZED VIEW IF EXISTS `"+dbName+"`.`"+name+"`"); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Materialized view dropped",
	})
}

// writableDB resolves the env pool and enforces admin write access (read-only guard).
func (h *MaterializedViewHandler) writableDB(w http.ResponseWriter, r *http.Request) (*sql.DB, bool) {
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
