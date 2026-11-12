package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type QueriesHandler struct {
	pools *pool.Manager
}

func NewQueriesHandler(pools *pool.Manager) *QueriesHandler {
	return &QueriesHandler{pools: pools}
}

// ProcessList returns currently running queries (SHOW PROCESSLIST).
func (h *QueriesHandler) ProcessList(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	results, err := queryToMaps(db, r, "SHOW PROCESSLIST")
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query process list: "+err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, results)
}

// CurrentQueries returns running queries via SHOW PROC "/current_queries" which has richer detail.
func (h *QueriesHandler) CurrentQueries(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	results, err := queryToMaps(db, r, `SHOW PROC "/current_queries"`)
	if err != nil {
		// Fallback to SHOW PROCESSLIST
		results, err = queryToMaps(db, r, "SHOW PROCESSLIST")
		if err != nil {
			stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query current queries: "+err.Error())
			return
		}
	}

	stream.WriteJSON(w, http.StatusOK, results)
}

// SlowQueries returns recent slow queries. Tries multiple sources in order of preference.
func (h *QueriesHandler) SlowQueries(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "100"
	}

	// Try 1: information_schema.slow_query (newer StarRocks versions)
	query := `SELECT QueryId, StartTime, QueryTime, ScanBytes, ScanRows, ReturnRows,
		CpuCostSeconds, MemCostBytes, StmtType, State,
		Db, User, Digest, PlanCpuCost, PlanMemCost, Stmt
	FROM information_schema.slow_query
	ORDER BY StartTime DESC LIMIT ` + limit
	results, err := queryToMaps(db, r, query)
	if err == nil {
		stream.WriteJSON(w, http.StatusOK, results)
		return
	}

	// Try 2: starrocks_audit_db__.starrocks_audit_tbl__ (audit plugin)
	query2 := `SELECT queryId, timestamp, queryTime, scanBytes, scanRows, returnRows,
		cpuCostNs, memCostBytes, stmtType, state, db, user, stmt
	FROM starrocks_audit_db__.starrocks_audit_tbl__
	ORDER BY timestamp DESC LIMIT ` + limit
	results, err = queryToMaps(db, r, query2)
	if err == nil {
		stream.WriteJSON(w, http.StatusOK, results)
		return
	}

	// No slow query source available — return empty with info
	stream.WriteJSON(w, http.StatusOK, []map[string]interface{}{})
}

// KillQuery kills a running query by its connection ID.
func (h *QueriesHandler) KillQuery(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type killReq struct {
		ConnectionID string `json:"connection_id"`
	}

	var req killReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ConnectionID == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "connection_id is required")
		return
	}

	// Sanitize: only allow numeric connection IDs
	for _, c := range req.ConnectionID {
		if c < '0' || c > '9' {
			stream.WriteJSONError(w, http.StatusBadRequest, "invalid connection_id")
			return
		}
	}

	sql := "KILL " + strings.TrimSpace(req.ConnectionID)
	_, err = db.ExecContext(r.Context(), sql)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Query killed successfully",
	})
}
