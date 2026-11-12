package handler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type BatchLoadHandler struct {
	pools *pool.Manager
}

func NewBatchLoadHandler(pools *pool.Manager) *BatchLoadHandler {
	return &BatchLoadHandler{pools: pools}
}

// List returns recent load jobs (all types: Broker Load, Stream Load, Insert, etc.) for a database.
// Tries information_schema.loads first (StarRocks 3.x, includes Stream Load), falls back to SHOW LOAD.
func (h *BatchLoadHandler) List(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbName := r.URL.Query().Get("db")

	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "50"
	}

	// Try information_schema.loads first — it includes ALL load types (Stream Load, Broker Load, etc.)
	infoQuery := "SELECT" +
		" DB_NAME as DbName," +
		" LABEL as Label," +
		" TABLE_NAME as TableName," +
		" STATE as State," +
		" TYPE as Type," +
		" PROGRESS as Progress," +
		" CREATE_TIME as CreateTime," +
		" LOAD_START_TIME as LoadStartTime," +
		" LOAD_FINISH_TIME as LoadFinishTime," +
		" ERROR_MSG as ErrorMsg," +
		" FILTERED_ROWS as FilteredRows," +
		" SINK_ROWS as LoadedRows," +
		" SCAN_ROWS as ScanRows," +
		" TRACKING_SQL as TrackingUrl" +
		" FROM information_schema.loads"
	if dbName != "" {
		infoQuery += " WHERE DB_NAME = '" + dbName + "'"
	} else {
		infoQuery += " WHERE DB_NAME NOT IN ('_statistics_', 'information_schema', 'sys')" +
			" AND DB_NAME NOT LIKE '\\_%'"
	}
	infoQuery += " ORDER BY CREATE_TIME DESC" +
		" LIMIT " + limit

	results, err := queryToMaps(db, r, infoQuery)
	if err == nil {
		stream.WriteJSON(w, http.StatusOK, results)
		return
	}

	// Fallback to SHOW LOAD (older StarRocks versions)
	if dbName != "" {
		query := "SHOW LOAD FROM `" + dbName + "` ORDER BY CreateTime DESC LIMIT " + limit
		results, err = queryToMaps(db, r, query)
		if err != nil {
			stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query load jobs: "+err.Error())
			return
		}
		stream.WriteJSON(w, http.StatusOK, results)
		return
	}

	// No specific db — iterate all databases
	dbRows, dbErr := db.QueryContext(r.Context(), "SHOW DATABASES")
	if dbErr != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list databases: "+dbErr.Error())
		return
	}
	defer dbRows.Close()

	var allResults []map[string]interface{}
	for dbRows.Next() {
		var name interface{}
		if err := dbRows.Scan(&name); err != nil {
			continue
		}
		var dn string
		switch v := name.(type) {
		case []byte:
			dn = string(v)
		case string:
			dn = v
		}
		if dn == "information_schema" || dn == "sys" || strings.HasPrefix(dn, "_") {
			continue
		}
		query := "SHOW LOAD FROM `" + dn + "` ORDER BY CreateTime DESC LIMIT " + limit
		dbResults, err := queryToMaps(db, r, query)
		if err != nil {
			continue
		}
		allResults = append(allResults, dbResults...)
	}
	if allResults == nil {
		allResults = []map[string]interface{}{}
	}
	stream.WriteJSON(w, http.StatusOK, allResults)
}

// Execute runs a batch load command (LOAD LABEL or INSERT INTO).
func (h *BatchLoadHandler) Execute(w http.ResponseWriter, r *http.Request) {
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

	type execReq struct {
		SQL      string `json:"sql"`
		Database string `json:"database"`
	}

	var req execReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sql := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(sql)

	// Only allow batch load related commands
	allowed := false
	for _, prefix := range []string{
		"LOAD LABEL",
		"INSERT INTO",
		"SHOW LOAD",
	} {
		if strings.HasPrefix(upper, prefix) {
			allowed = true
			break
		}
	}

	if !allowed {
		stream.WriteJSONError(w, http.StatusForbidden,
			"only LOAD LABEL and INSERT INTO commands are allowed")
		return
	}

	// SHOW LOAD returns rows
	if strings.HasPrefix(upper, "SHOW LOAD") {
		results, err := queryToMaps(db, r, sql)
		if err != nil {
			stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		stream.WriteJSON(w, http.StatusOK, results)
		return
	}

	// LOAD / INSERT — exec with a selected database (some statements require it)
	if err = execWithDatabase(r.Context(), db, req.Database, sql); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Load job submitted successfully",
	})
}

// ExportFiles unloads a query result to object storage via INSERT INTO FILES(...).
// The frontend builds the full statement (path, format, SELECT). For GCS the cluster
// authenticates via core-site.xml, so no inline credentials are needed.
func (h *BatchLoadHandler) ExportFiles(w http.ResponseWriter, r *http.Request) {
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
		SQL      string `json:"sql"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sql := strings.TrimSpace(req.SQL)
	if !strings.HasPrefix(strings.ToUpper(sql), "INSERT INTO FILES") {
		stream.WriteJSONError(w, http.StatusForbidden, "only INSERT INTO FILES(...) statements are allowed")
		return
	}

	if err := execWithDatabase(r.Context(), db, req.Database, sql); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Export completed",
	})
}

// Cancel cancels a batch load job by label.
func (h *BatchLoadHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type cancelReq struct {
		Database string `json:"database"`
		Label    string `json:"label"`
	}

	var req cancelReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Database == "" || req.Label == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "database and label are required")
		return
	}

	sql := `CANCEL LOAD FROM ` + "`" + req.Database + "`" + ` WHERE LABEL = "` + req.Label + `"`
	_, err = db.ExecContext(r.Context(), sql)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Load job cancelled successfully",
	})
}

// StreamLoad proxies a file upload to StarRocks FE HTTP Stream Load API.
func (h *BatchLoadHandler) StreamLoad(w http.ResponseWriter, r *http.Request) {
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

	// 512 MB max upload
	if err := r.ParseMultipartForm(512 << 20); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "failed to parse form: "+err.Error())
		return
	}

	database := r.FormValue("database")
	table := r.FormValue("table")
	if database == "" || table == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "database and table are required")
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "file is required: "+err.Error())
		return
	}
	defer file.Close()

	// Read file into memory so we can resend on redirect
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to read file: "+err.Error())
		return
	}

	// Build FE HTTP URL: http://<fe_host>:<fe_http_port>/api/<db>/<table>/_stream_load
	feHost := extractHost(env.Hosts[0])
	feURL := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", feHost, env.FEHttpPort, database, table)

	// Auth header (StarRocks uses HTTP Basic Auth for stream load)
	auth := base64.StdEncoding.EncodeToString([]byte(env.User + ":" + env.Password))

	// Collect load option headers from form fields
	optHeaders := http.Header{}
	for _, key := range []string{
		"format", "column_separator", "row_separator",
		"columns", "where", "max_filter_ratio",
		"skip_header", "trim_space", "enclose", "escape",
		"strict_mode", "timezone", "jsonpaths", "json_root",
		"strip_outer_array", "partial_update",
	} {
		if v := r.FormValue(key); v != "" {
			optHeaders.Set(key, v)
		}
	}
	if label := r.FormValue("label"); label != "" {
		optHeaders.Set("label", label)
	}

	// Helper to build a PUT request with all headers
	buildReq := func(url string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(r.Context(), "PUT", url, bytes.NewReader(fileBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Basic "+auth)
		req.Header.Set("Expect", "100-continue")
		for k, vs := range optHeaders {
			for _, v := range vs {
				req.Header.Set(k, v)
			}
		}
		return req, nil
	}

	// Do NOT follow redirects automatically — Go drops the body on 307.
	// Instead, handle the redirect manually so we can resend the file.
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	putReq, err := buildReq(feURL)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to create request: "+err.Error())
		return
	}

	resp, err := client.Do(putReq)
	if err != nil {
		stream.WriteJSONError(w, http.StatusBadGateway, "stream load request failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	// Handle 307 redirect: FE redirects to BE — resend file to the new URL
	if resp.StatusCode == http.StatusTemporaryRedirect {
		loc := resp.Header.Get("Location")
		if loc == "" {
			stream.WriteJSONError(w, http.StatusBadGateway, "FE returned 307 but no Location header")
			return
		}
		resp.Body.Close()

		putReq2, err := buildReq(loc)
		if err != nil {
			stream.WriteJSONError(w, http.StatusInternalServerError, "failed to create redirect request: "+err.Error())
			return
		}

		resp, err = client.Do(putReq2)
		if err != nil {
			stream.WriteJSONError(w, http.StatusBadGateway, "stream load redirect request failed: "+err.Error())
			return
		}
		defer resp.Body.Close()
	}

	// Forward the StarRocks response as-is
	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// extractHost returns the host part from a "host:port" string.
func extractHost(hostPort string) string {
	idx := strings.LastIndex(hostPort, ":")
	if idx < 0 {
		return hostPort
	}
	return hostPort[:idx]
}
