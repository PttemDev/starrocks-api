package handler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type SchemaHandler struct {
	pools *pool.Manager
}

func NewSchemaHandler(pools *pool.Manager) *SchemaHandler {
	return &SchemaHandler{pools: pools}
}

// Databases returns all database names.
func (h *SchemaHandler) Databases(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	names, err := queryColumn(db, r, "SHOW DATABASES")
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list databases: "+err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, names)
}

// Tables returns all table names for a database.
func (h *SchemaHandler) Tables(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := r.URL.Query().Get("db")
	if dbName == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "db query parameter is required")
		return
	}

	names, err := queryColumn(db, r, "SHOW TABLES FROM `"+dbName+"`")
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list tables: "+err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, names)
}

// Columns returns column metadata for a specific table.
func (h *SchemaHandler) Columns(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := r.URL.Query().Get("db")
	tableName := r.URL.Query().Get("table")
	if dbName == "" || tableName == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "db and table query parameters are required")
		return
	}

	rows, err := db.QueryContext(r.Context(), "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", dbName, tableName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list columns: "+err.Error())
		return
	}
	defer rows.Close()

	type columnInfo struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable string `json:"nullable"`
		Comment  string `json:"comment"`
	}

	var result []columnInfo
	for rows.Next() {
		var c columnInfo
		if err := rows.Scan(&c.Name, &c.Type, &c.Nullable, &c.Comment); err != nil {
			stream.WriteJSONError(w, http.StatusInternalServerError, "scan error: "+err.Error())
			return
		}
		result = append(result, c)
	}

	if result == nil {
		result = []columnInfo{}
	}

	stream.WriteJSON(w, http.StatusOK, result)
}

// TableDetails returns detailed table information for a database.
func (h *SchemaHandler) TableDetails(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := r.URL.Query().Get("db")
	if dbName == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "db query parameter is required")
		return
	}

	query := `SELECT
		TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_ROWS,
		AVG_ROW_LENGTH, DATA_LENGTH, CREATE_TIME, UPDATE_TIME,
		TABLE_COMMENT
	FROM information_schema.TABLES
	WHERE TABLE_SCHEMA = ?
	ORDER BY TABLE_NAME`

	rows, err := db.QueryContext(r.Context(), query, dbName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query tables: "+err.Error())
		return
	}
	defer rows.Close()

	type tableDetail struct {
		Name         string      `json:"name"`
		Type         string      `json:"type"`
		Engine       interface{} `json:"engine"`
		Rows         interface{} `json:"rows"`
		AvgRowLength interface{} `json:"avg_row_length"`
		DataLength   interface{} `json:"data_length"`
		CreateTime   interface{} `json:"create_time"`
		UpdateTime   interface{} `json:"update_time"`
		Comment      interface{} `json:"comment"`
	}

	var results []tableDetail
	for rows.Next() {
		var t tableDetail
		if err := rows.Scan(&t.Name, &t.Type, &t.Engine, &t.Rows,
			&t.AvgRowLength, &t.DataLength, &t.CreateTime, &t.UpdateTime,
			&t.Comment); err != nil {
			stream.WriteJSONError(w, http.StatusInternalServerError, "scan error: "+err.Error())
			return
		}
		// Convert []byte to string for nullable columns
		for _, ptr := range []*interface{}{&t.Engine, &t.CreateTime, &t.UpdateTime, &t.Comment} {
			if b, ok := (*ptr).([]byte); ok {
				*ptr = string(b)
			}
		}
		results = append(results, t)
	}

	// Detect materialized views: StarRocks reports them as VIEW in information_schema.TABLES,
	// but they exist in information_schema.materialized_views.
	mvSet := make(map[string]bool)
	mvRows, mvErr := db.QueryContext(r.Context(),
		"SELECT TABLE_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = ?", dbName)
	if mvErr == nil {
		defer mvRows.Close()
		for mvRows.Next() {
			var name interface{}
			if err := mvRows.Scan(&name); err == nil {
				switch v := name.(type) {
				case []byte:
					mvSet[string(v)] = true
				case string:
					mvSet[v] = true
				}
			}
		}
	}
	for i := range results {
		if results[i].Type == "VIEW" && mvSet[results[i].Name] {
			results[i].Type = "MATERIALIZED VIEW"
		}
	}

	if results == nil {
		results = []tableDetail{}
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// AddColumn executes an ALTER TABLE ADD COLUMN command.
func (h *SchemaHandler) AddColumn(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())

	// Check read-only
	env, ok := h.pools.GetEnv(envName)
	if !ok {
		stream.WriteJSONError(w, http.StatusInternalServerError, "unknown environment")
		return
	}
	if env.ReadOnly {
		stream.WriteJSONError(w, http.StatusForbidden, "this environment is read-only")
		return
	}

	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type addColumnReq struct {
		Database string `json:"database"`
		Table    string `json:"table"`
		SQL      string `json:"sql"`
	}

	var req addColumnReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Database == "" || req.Table == "" || req.SQL == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "database, table, and sql are required")
		return
	}

	// Only allow ALTER TABLE commands
	upper := strings.ToUpper(strings.TrimSpace(req.SQL))
	if !strings.HasPrefix(upper, "ALTER TABLE") {
		stream.WriteJSONError(w, http.StatusForbidden, "only ALTER TABLE commands are allowed")
		return
	}

	_, err = db.ExecContext(r.Context(), req.SQL)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Column added successfully",
	})
}

// AdminExec executes cluster administration commands (ALTER SYSTEM).
func (h *SchemaHandler) AdminExec(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type adminReq struct {
		SQL string `json:"sql"`
	}

	var req adminReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sql := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(sql)

	// Only allow specific admin commands
	allowed := false
	for _, prefix := range []string{
		"ALTER SYSTEM ADD FOLLOWER",
		"ALTER SYSTEM ADD OBSERVER",
		"ALTER SYSTEM DROP FOLLOWER",
		"ALTER SYSTEM DROP OBSERVER",
		"ALTER SYSTEM ADD BACKEND",
		"ALTER SYSTEM DROP BACKEND",
		"ALTER SYSTEM DECOMMISSION BACKEND",
		"DROP TABLE",
		"CREATE DATABASE",
		"DROP DATABASE",
	} {
		if strings.HasPrefix(upper, prefix) {
			allowed = true
			break
		}
	}

	if !allowed {
		stream.WriteJSONError(w, http.StatusForbidden,
			"only ALTER SYSTEM node management commands are allowed")
		return
	}

	_, err = db.ExecContext(r.Context(), sql)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Command executed successfully",
	})
}

// CreateTable executes a CREATE TABLE command.
func (h *SchemaHandler) CreateTable(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())

	env, ok := h.pools.GetEnv(envName)
	if !ok {
		stream.WriteJSONError(w, http.StatusInternalServerError, "unknown environment")
		return
	}
	if env.ReadOnly {
		stream.WriteJSONError(w, http.StatusForbidden, "this environment is read-only")
		return
	}

	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type createTableReq struct {
		SQL string `json:"sql"`
	}

	var req createTableReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sqlStr := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(sqlStr)
	if !strings.HasPrefix(upper, "CREATE TABLE") && !strings.HasPrefix(upper, "CREATE EXTERNAL TABLE") {
		stream.WriteJSONError(w, http.StatusForbidden, "only CREATE TABLE commands are allowed")
		return
	}

	_, err = db.ExecContext(r.Context(), sqlStr)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Table created successfully",
	})
}

// queryColumn executes a query and returns the first column of each row as a string slice.
func queryColumn(db *sql.DB, r *http.Request, query string) ([]string, error) {
	rows, err := db.QueryContext(r.Context(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case []byte:
			result = append(result, string(v))
		case string:
			result = append(result, v)
		default:
			result = append(result, "")
		}
	}

	if result == nil {
		result = []string{}
	}

	return result, rows.Err()
}

// ShowCreateDDL returns the creation DDL for a table, view, or materialized view.
func (h *SchemaHandler) ShowCreateDDL(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := r.URL.Query().Get("db")
	tableName := r.URL.Query().Get("table")
	tableType := r.URL.Query().Get("type") // "BASE TABLE", "VIEW", "MATERIALIZED VIEW", etc.
	if dbName == "" || tableName == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "db and table query parameters are required")
		return
	}

	qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, tableName)

	// Pick the right SHOW CREATE command based on type
	var query string
	switch strings.ToUpper(tableType) {
	case "MATERIALIZED VIEW":
		query = "SHOW CREATE MATERIALIZED VIEW " + qualifiedName
	case "VIEW":
		query = "SHOW CREATE VIEW " + qualifiedName
	default:
		query = "SHOW CREATE TABLE " + qualifiedName
	}

	rows, err := db.QueryContext(r.Context(), query)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if !rows.Next() {
		stream.WriteJSONError(w, http.StatusNotFound, "no DDL found")
		return
	}

	// SHOW CREATE returns 2 columns: name + DDL
	values := make([]interface{}, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// The DDL is in the second column
	ddl := ""
	if len(values) >= 2 {
		switch v := values[1].(type) {
		case []byte:
			ddl = string(v)
		case string:
			ddl = v
		}
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"name": tableName,
		"type": tableType,
		"ddl":  ddl,
	})
}
