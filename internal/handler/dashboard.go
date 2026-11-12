package handler

import (
	"database/sql"
	"net/http"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type DashboardHandler struct {
	pools *pool.Manager
}

func NewDashboardHandler(pools *pool.Manager) *DashboardHandler {
	return &DashboardHandler{pools: pools}
}

// Nodes returns StarRocks backend/frontend node statuses.
func (h *DashboardHandler) Nodes(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type nodeInfo struct {
		Type string                 `json:"type"`
		Data []map[string]interface{} `json:"data"`
	}

	var nodes []nodeInfo

	// Get backends
	backends, err := queryToMaps(db, r, "SHOW BACKENDS")
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query backends: "+err.Error())
		return
	}
	nodes = append(nodes, nodeInfo{Type: "backend", Data: backends})

	// Get frontends
	frontends, err := queryToMaps(db, r, "SHOW FRONTENDS")
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query frontends: "+err.Error())
		return
	}
	nodes = append(nodes, nodeInfo{Type: "frontend", Data: frontends})

	stream.WriteJSON(w, http.StatusOK, nodes)
}

// Ingestion returns routine load job history.
func (h *DashboardHandler) Ingestion(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := stripBackticks(r.URL.Query().Get("db"))
	query := "SHOW ROUTINE LOAD"
	if dbName != "" {
		query = "SHOW ROUTINE LOAD FROM `" + dbName + "`"
	}

	results, err := queryToMaps(db, r, query)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query ingestion: "+err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, results)
}

// Routines returns all routine load jobs (all states — RUNNING, PAUSED, STOPPED,
// CANCELLED, …) across all user databases; the per-database SHOW ALL ROUTINE LOAD
// calls run concurrently.
func (h *DashboardHandler) Routines(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	results, err := listRoutineLoads(r.Context(), db, "", "")
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list routine loads: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// queryToMaps executes a query and returns all rows as []map[string]interface{}.
// This is used for small result sets (dashboard data), not for large streaming queries.
func queryToMaps(db *sql.DB, r *http.Request, query string) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(r.Context(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if results == nil {
		results = []map[string]interface{}{}
	}

	return results, nil
}
