package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"starrocks-api/internal/middleware"
	"starrocks-api/internal/pool"
	"starrocks-api/internal/stream"
)

type RoutineLoadHandler struct {
	pools *pool.Manager
}

func NewRoutineLoadHandler(pools *pool.Manager) *RoutineLoadHandler {
	return &RoutineLoadHandler{pools: pools}
}

// allowedCommands lists the only SQL command prefixes this endpoint will execute.
var allowedCommands = []string{
	"PAUSE ROUTINE LOAD",
	"RESUME ROUTINE LOAD",
	"STOP ROUTINE LOAD",
	"ALTER ROUTINE LOAD",
	"CREATE ROUTINE LOAD",
	"SHOW ROUTINE LOAD",
	"SHOW ALL ROUTINE LOAD",
}

type routineLoadRequest struct {
	SQL string `json:"sql"`
}

// Execute runs a routine load management command.
// This endpoint allows specific management commands even on read-only environments,
// because routine load management is an operational task, not a data mutation.
func (h *RoutineLoadHandler) Execute(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())

	var req routineLoadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	sql := strings.TrimSpace(req.SQL)
	if sql == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "sql is required")
		return
	}

	// Only allow specific routine load commands. Classify by the statement, not a
	// leading comment banner an operator may have left in the editor.
	upper := strings.ToUpper(stripLeadingComments(sql))
	allowed := false
	for _, cmd := range allowedCommands {
		if strings.HasPrefix(upper, cmd) {
			allowed = true
			break
		}
	}
	if !allowed {
		stream.WriteJSONError(w, http.StatusForbidden,
			"only routine load management commands are allowed through this endpoint")
		return
	}

	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// For SHOW commands, return result rows
	if strings.HasPrefix(upper, "SHOW") {
		rows, err := db.QueryContext(r.Context(), sql)
		if err != nil {
			stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		defer rows.Close()

		results, err := rowsToMaps(rows)
		if err != nil {
			stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		stream.WriteJSON(w, http.StatusOK, results)
		return
	}

	// For action commands (CREATE, PAUSE, RESUME, STOP, ALTER), execute and return status
	result, err := db.ExecContext(r.Context(), sql)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	affected, _ := result.RowsAffected()
	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"rows_affected": affected,
		"message":       "Command executed successfully",
	})
}

// alterableJobProperties are the PROPERTIES keys ALTER ROUTINE LOAD accepts.
// Verified against StarRocks 4.0.4 — anything else (format, partial_update,
// trim_space, log_rejected_record_num, …) fails analysis with "<key> is invalid
// property" and needs a re-create instead.
var alterableJobProperties = map[string]bool{
	"desired_concurrent_number":  true,
	"max_error_number":           true,
	"max_filter_ratio":           true,
	"max_batch_interval":         true,
	"max_batch_rows":             true,
	"max_batch_size":             true,
	"task_consume_second":        true,
	"task_timeout_second":        true,
	"jsonpaths":                  true,
	"json_root":                  true,
	"strip_outer_array":          true,
	"strict_mode":                true,
	"timezone":                   true,
	"pause_on_fatal_parse_error": true,
}

// alterableSourceProperty reports whether a FROM KAFKA(...) key can be altered.
// kafka_broker_list is accepted (the job keeps its offsets and the next scheduler
// round connects to the new brokers); kafka_topic is not — that is a re-create.
func alterableSourceProperty(key string) bool {
	switch key {
	case "kafka_broker_list", "kafka_partitions", "kafka_offsets", "confluent.schema.registry.url":
		return true
	}
	return strings.HasPrefix(key, "property.") && len(key) > len("property.")
}

type alterRoutineLoadRequest struct {
	Database string `json:"database"`
	Name     string `json:"name"`
	// Columns is the body of COLUMNS(...) and Where the predicate of WHERE; both
	// optional and passed through verbatim (admin-only endpoint).
	Columns              string            `json:"columns"`
	Where                string            `json:"where"`
	JobProperties        map[string]string `json:"job_properties"`
	DataSourceProperties map[string]string `json:"data_source_properties"`
	// Resume after altering. Nil resumes only a job that was active before the
	// call, so a job an operator paused on purpose stays paused.
	Resume *bool `json:"resume"`
}

// Alter changes a routine load job in place: PAUSE (if it is active) → ALTER →
// RESUME. StarRocks only alters PAUSED jobs, so doing all three in one request
// keeps a half-finished edit from leaving a job paused behind the user's back.
// Like Execute, this is allowed on read-only environments (operational task).
func (h *RoutineLoadHandler) Alter(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())

	var req alterRoutineLoadRequest
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
	for k := range req.JobProperties {
		if !alterableJobProperties[k] {
			stream.WriteJSONError(w, http.StatusBadRequest, "property "+k+" cannot be altered; re-create the job to change it")
			return
		}
	}
	for k := range req.DataSourceProperties {
		if !alterableSourceProperty(k) {
			stream.WriteJSONError(w, http.StatusBadRequest, "source property "+k+" cannot be altered; re-create the job to change it")
			return
		}
	}

	target := "`" + dbName + "`.`" + name + "`"
	stmt := buildAlterRoutineLoad(target, req)
	if stmt == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "nothing to alter")
		return
	}

	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Finish the sequence even if the client goes away mid-way: abandoning it after
	// PAUSE would leave a live job stopped consuming.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer conn.Close()

	state, err := activeRoutineLoadState(ctx, conn, target)
	if err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if state == "" {
		stream.WriteJSONError(w, http.StatusConflict,
			"job "+dbName+"."+name+" is not active (stopped or cancelled jobs cannot be altered — re-create it)")
		return
	}
	wasActive := state != "PAUSED"
	resume := wasActive
	if req.Resume != nil {
		resume = *req.Resume
	}

	executed := []string{}
	run := func(q string) error {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return err
		}
		executed = append(executed, q)
		return nil
	}

	if wasActive {
		if err := run("PAUSE ROUTINE LOAD FOR " + target); err != nil {
			stream.WriteJSONError(w, http.StatusUnprocessableEntity, "pause failed: "+err.Error())
			return
		}
	}
	if err := run(stmt); err != nil {
		// A failed ALTER changes nothing, so put the job back the way we found it.
		msg := "alter failed: " + err.Error()
		if wasActive {
			if rerr := run("RESUME ROUTINE LOAD FOR " + target); rerr != nil {
				msg += "; the job is left PAUSED (resume failed: " + rerr.Error() + ")"
			} else {
				msg += "; the job was resumed with its previous settings"
			}
		}
		slog.Warn("routine load alter failed", "env", envName, "job", dbName+"."+name, "err", err)
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, msg)
		return
	}

	resumed := false
	warning := ""
	if resume {
		if err := run("RESUME ROUTINE LOAD FOR " + target); err != nil {
			warning = "settings applied, but RESUME failed: " + err.Error() + " — the job is PAUSED"
		} else {
			resumed = true
		}
	}
	slog.Info("routine load altered", "env", envName, "job", dbName+"."+name, "resumed", resumed)
	resp := map[string]interface{}{
		"success":    true,
		"message":    "Routine load updated",
		"statements": executed,
		"resumed":    resumed,
	}
	if warning != "" {
		resp["warning"] = warning
	}
	stream.WriteJSON(w, http.StatusOK, resp)
}

// buildAlterRoutineLoad renders the ALTER statement, or "" when there is nothing
// to change. Keys are already whitelisted; values are single-quoted because some
// (jsonpaths) are JSON full of double quotes.
func buildAlterRoutineLoad(target string, req alterRoutineLoadRequest) string {
	var clauses []string
	var load []string
	if c := strings.TrimSpace(req.Columns); c != "" {
		load = append(load, "COLUMNS("+c+")")
	}
	if wh := strings.TrimSpace(req.Where); wh != "" {
		load = append(load, "WHERE "+wh)
	}
	if len(load) > 0 {
		clauses = append(clauses, strings.Join(load, ",\n"))
	}
	if len(req.JobProperties) > 0 {
		clauses = append(clauses, "PROPERTIES (\n"+propertyList(req.JobProperties)+"\n)")
	}
	if len(req.DataSourceProperties) > 0 {
		clauses = append(clauses, "FROM KAFKA (\n"+propertyList(req.DataSourceProperties)+"\n)")
	}
	if len(clauses) == 0 {
		return ""
	}
	return "ALTER ROUTINE LOAD FOR " + target + "\n" + strings.Join(clauses, "\n")
}

// propertyList renders `"key" = 'value'` pairs in key order (stable statements).
func propertyList(props map[string]string) string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = `  "` + k + `" = ` + sqlStringLiteral(props[k])
	}
	return strings.Join(parts, ",\n")
}

// sqlStringLiteral single-quotes s with backslash escaping (StarRocks follows
// MySQL string literal rules).
func sqlStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// activeRoutineLoadState returns the state of the job's non-final instance
// (SHOW ROUTINE LOAD without ALL skips STOPPED/CANCELLED), or "" if there is none.
func activeRoutineLoadState(ctx context.Context, conn *sql.Conn, target string) (string, error) {
	rows, err := conn.QueryContext(ctx, "SHOW ROUTINE LOAD FOR "+target)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	results, err := rowsToMaps(rows)
	if err != nil {
		return "", err
	}
	for _, row := range results {
		if s, _ := row["State"].(string); s != "" {
			return strings.ToUpper(s), nil
		}
	}
	return "", nil
}

// ListForDB returns routine load jobs for a specific database.
func (h *RoutineLoadHandler) ListForDB(w http.ResponseWriter, r *http.Request) {
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

	results, err := listRoutineLoads(r.Context(), db, dbName, r.URL.Query().Get("table"))
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to query routine loads: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// listRoutineLoads returns routine load jobs in every state (SHOW ALL), optionally
// narrowed to one database and/or target table. Without a database it fans out
// over every user database, a few at a time.
//
// information_schema.routine_load_jobs would be a single query, but it is served
// by a BE scan that calls back into the FE, and on the test cluster it stalled
// for 20s+ now and then; SHOW ALL is answered from FE metadata alone (~30ms).
func listRoutineLoads(ctx context.Context, db *sql.DB, dbName, table string) ([]map[string]interface{}, error) {
	databases := []string{dbName}
	if dbName == "" {
		var err error
		if databases, err = userDatabases(ctx, db); err != nil {
			return nil, err
		}
	}

	results := make([][]map[string]interface{}, len(databases))
	errs := make([]error, len(databases))
	sem := make(chan struct{}, 4) // leave pool headroom for auto-refreshing pages
	var wg sync.WaitGroup
	for i, name := range databases {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rows, err := db.QueryContext(ctx, "SHOW ALL ROUTINE LOAD FROM `"+stripBackticks(name)+"`")
			if err != nil {
				errs[i] = err
				return
			}
			defer rows.Close()
			results[i], errs[i] = rowsToMaps(rows)
		}(i, name)
	}
	wg.Wait()

	all := []map[string]interface{}{}
	for i, rows := range results {
		if errs[i] != nil {
			if dbName != "" {
				return nil, errs[i]
			}
			// One unreadable database shouldn't hide every other job.
			slog.Warn("skipping database in routine load listing", "db", databases[i], "err", errs[i])
			continue
		}
		for _, row := range rows {
			if tn, _ := row["TableName"].(string); table == "" || tn == table {
				all = append(all, row)
			}
		}
	}
	return all, nil
}

// userDatabases lists databases, minus the system ones that never hold jobs.
func userDatabases(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name == "information_schema" || name == "sys" || name == "_statistics_" {
			continue
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// rowsToMaps converts *sql.Rows to []map[string]interface{}.
func rowsToMaps(rows interface{ Columns() ([]string, error); Next() bool; Scan(...interface{}) error; Err() error }) ([]map[string]interface{}, error) {
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

	if results == nil {
		results = []map[string]interface{}{}
	}
	return results, rows.Err()
}
