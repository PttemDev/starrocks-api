package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type ExportHandler struct {
	pools *pool.Manager
}

func NewExportHandler(pools *pool.Manager) *ExportHandler {
	return &ExportHandler{pools: pools}
}

type exportRequest struct {
	SQL              string            `json:"sql"`
	Format           string            `json:"format"`            // csv, excel, json
	SessionVariables map[string]string `json:"session_variables"` // optional SET key=value pairs
}

func (h *ExportHandler) Export(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())

	var req exportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "sql is required")
		return
	}

	format := strings.ToLower(req.Format)
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "excel" && format != "json" {
		stream.WriteJSONError(w, http.StatusBadRequest, "format must be csv, excel, or json")
		return
	}

	// Check read-only
	env, ok := h.pools.GetEnv(envName)
	if !ok {
		stream.WriteJSONError(w, http.StatusBadRequest, "unknown environment")
		return
	}
	if env.ReadOnly {
		upper := strings.ToUpper(stripLeadingComments(req.SQL))
		for _, stmt := range dangerousStatements {
			if strings.HasPrefix(upper, stmt) {
				stream.WriteJSONError(w, http.StatusForbidden,
					"environment is read-only; only SELECT and SHOW queries are allowed")
				return
			}
		}
	}

	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	rows, cleanup, err := queryWithSessionVars(r.Context(), db, req.SQL, req.SessionVariables)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	defer cleanup()
	defer rows.Close()

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("export_%s_%s", envName, timestamp)

	switch format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.csv"`, filename))
		w.WriteHeader(http.StatusOK)
		if err := stream.ExportCSV(w, rows); err != nil {
			return
		}

	case "excel":
		w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.xlsx"`, filename))
		w.WriteHeader(http.StatusOK)
		if err := stream.ExportExcel(w, rows); err != nil {
			return
		}

	case "json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.json"`, filename))
		w.WriteHeader(http.StatusOK)
		if err := stream.ExportJSON(w, rows); err != nil {
			return
		}
	}
}
