package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

// dangerousStatements lists SQL keywords that modify data.
var dangerousStatements = []string{
	"DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
	"CREATE", "GRANT", "REVOKE",
}

// stripLeadingComments drops leading whitespace and SQL comments so the
// prefix check below classifies the first real keyword. Without it a query
// that opens with a "-- ..." banner slips past the read-only guard entirely.
func stripLeadingComments(sql string) string {
	s := strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(s, "--"), strings.HasPrefix(s, "#"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return ""
			}
			s = strings.TrimSpace(s[i+1:])
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s[2:], "*/")
			if i < 0 {
				return ""
			}
			s = strings.TrimSpace(s[i+4:])
		default:
			return s
		}
	}
}

type QueryHandler struct {
	pools *pool.Manager
}

func NewQueryHandler(pools *pool.Manager) *QueryHandler {
	return &QueryHandler{pools: pools}
}

type queryRequest struct {
	SQL              string            `json:"sql"`
	Format           string            `json:"format"`            // "ndjson" (default) or "json"
	SessionVariables map[string]string `json:"session_variables"` // optional SET key=value pairs
}

func (h *QueryHandler) Execute(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())

	var req queryRequest
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
		format = "ndjson"
	}
	if format != "ndjson" && format != "json" {
		stream.WriteJSONError(w, http.StatusBadRequest, "format must be ndjson or json")
		return
	}

	// Check read-only mode
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

	if format == "json" {
		if err := stream.QueryResultBuffered(w, rows); err != nil {
			return
		}
	} else {
		if err := stream.QueryResultJSON(w, rows); err != nil {
			// Response already started, can't send error status.
			// Error is logged by the logging middleware.
			return
		}
	}
}
