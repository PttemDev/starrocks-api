package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

// PartitionHandler exposes table partition management: list partitions (SHOW
// PARTITIONS) and add/drop partitions (ALTER TABLE ... PARTITION). Useful for
// time-partitioned tables — see retention, prune old partitions, add new ones.
type PartitionHandler struct {
	pools *pool.Manager
}

func NewPartitionHandler(pools *pool.Manager) *PartitionHandler {
	return &PartitionHandler{pools: pools}
}

// List returns the partitions of a table via SHOW PARTITIONS.
func (h *PartitionHandler) List(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := stripBackticks(r.URL.Query().Get("db"))
	table := stripBackticks(r.URL.Query().Get("table"))
	if dbName == "" || table == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "db and table are required")
		return
	}

	results, err := queryToMaps(db, r, "SHOW PARTITIONS FROM `"+dbName+"`.`"+table+"`")
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, "failed to list partitions: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// Exec runs an ALTER TABLE ... ADD/DROP PARTITION statement built by the frontend.
func (h *PartitionHandler) Exec(w http.ResponseWriter, r *http.Request) {
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

	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sqlStmt := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(sqlStmt)
	if !strings.HasPrefix(upper, "ALTER TABLE") || !strings.Contains(upper, "PARTITION") {
		stream.WriteJSONError(w, http.StatusForbidden,
			"only ALTER TABLE ... PARTITION statements are allowed")
		return
	}

	if _, err := db.ExecContext(r.Context(), sqlStmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Partition operation applied",
	})
}
