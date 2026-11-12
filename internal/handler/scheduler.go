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

// execWithDatabase runs a statement with a current database set. Some statements
// (notably SUBMIT TASK) require a selected database even when every table name in
// them is fully qualified, otherwise StarRocks returns "No database selected".
// We set it on a dedicated connection so the USE doesn't leak across the pool.
func execWithDatabase(ctx context.Context, db *sql.DB, dbName, query string) error {
	dbName = strings.ReplaceAll(strings.TrimSpace(dbName), "`", "")
	if dbName == "" {
		_, err := db.ExecContext(ctx, query)
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "USE `"+dbName+"`"); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, query)
	return err
}

// SchedulerHandler manages StarRocks scheduled tasks (SUBMIT TASK ... SCHEDULE).
// Scheduling lives entirely in the StarRocks FE — no extra datastore or in-process
// cron is needed. We only issue SUBMIT TASK / DROP TASK and read back state from
// information_schema.tasks and information_schema.task_runs.
type SchedulerHandler struct {
	pools *pool.Manager
}

func NewSchedulerHandler(pools *pool.Manager) *SchedulerHandler {
	return &SchedulerHandler{pools: pools}
}

// List returns all scheduled tasks, enriched with the state of their most recent run.
func (h *SchedulerHandler) List(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	tasksQuery := "SELECT" +
		" TASK_NAME as TaskName," +
		" `DATABASE` as DbName," +
		" SCHEDULE as Schedule," +
		" DEFINITION as Definition," +
		" CREATE_TIME as CreateTime," +
		" EXPIRE_TIME as ExpireTime" +
		" FROM information_schema.tasks" +
		" ORDER BY CREATE_TIME DESC"

	tasks, err := queryToMaps(db, r, tasksQuery)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list tasks: "+err.Error())
		return
	}

	// Pull recent runs once and attach the latest run per task (rows are DESC by
	// create time, so the first row seen for a task name is its most recent run).
	runsQuery := "SELECT" +
		" TASK_NAME as TaskName," +
		" STATE as State," +
		" CREATE_TIME as CreateTime," +
		" FINISH_TIME as FinishTime," +
		" ERROR_MESSAGE as ErrorMessage" +
		" FROM information_schema.task_runs" +
		" ORDER BY CREATE_TIME DESC LIMIT 1000"

	latest := map[string]map[string]interface{}{}
	if runs, rErr := queryToMaps(db, r, runsQuery); rErr == nil {
		for _, run := range runs {
			name := toString(run["TaskName"])
			if name == "" {
				continue
			}
			if _, seen := latest[name]; !seen {
				latest[name] = run
			}
		}
	}

	for _, t := range tasks {
		name := toString(t["TaskName"])
		if run, ok := latest[name]; ok {
			t["LastState"] = run["State"]
			t["LastRunTime"] = run["CreateTime"]
			t["LastFinishTime"] = run["FinishTime"]
			t["LastError"] = run["ErrorMessage"]
		}
	}

	if tasks == nil {
		tasks = []map[string]interface{}{}
	}
	stream.WriteJSON(w, http.StatusOK, tasks)
}

// Runs returns the execution history of a single task.
func (h *SchedulerHandler) Runs(w http.ResponseWriter, r *http.Request) {
	envName := middleware.EnvFromContext(r.Context())
	db, err := h.pools.Get(envName)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	task := strings.TrimSpace(r.URL.Query().Get("task"))
	if task == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "task is required")
		return
	}

	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "50"
	}

	// Escape single quotes to keep the string literal intact.
	safeTask := strings.ReplaceAll(task, "'", "''")
	// Note: QUERY_ID is intentionally omitted — it is not queryable in StarRocks v3.3.
	query := "SELECT" +
		" TASK_NAME as TaskName," +
		" STATE as State," +
		" CREATE_TIME as CreateTime," +
		" FINISH_TIME as FinishTime," +
		" ERROR_CODE as ErrorCode," +
		" ERROR_MESSAGE as ErrorMessage," +
		" PROGRESS as Progress" +
		" FROM information_schema.task_runs" +
		" WHERE TASK_NAME = '" + safeTask + "'" +
		" ORDER BY CREATE_TIME DESC LIMIT " + limit

	results, err := queryToMaps(db, r, query)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to list task runs: "+err.Error())
		return
	}
	stream.WriteJSON(w, http.StatusOK, results)
}

// Create submits a new scheduled task. The frontend builds the full
// SUBMIT TASK ... SCHEDULE ... AS INSERT statement; we whitelist the prefix.
func (h *SchedulerHandler) Create(w http.ResponseWriter, r *http.Request) {
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

	sqlStmt := strings.TrimSpace(req.SQL)
	if !strings.HasPrefix(strings.ToUpper(sqlStmt), "SUBMIT TASK") {
		stream.WriteJSONError(w, http.StatusForbidden, "only SUBMIT TASK statements are allowed")
		return
	}

	if err := execWithDatabase(r.Context(), db, req.Database, sqlStmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Scheduled task created successfully",
	})
}

// Update edits a scheduled task. StarRocks has no ALTER TASK for the definition or
// schedule, so editing is drop-then-recreate: we DROP the existing task and run the
// new SUBMIT TASK statement (which the frontend rebuilds with the edited SQL/schedule).
func (h *SchedulerHandler) Update(w http.ResponseWriter, r *http.Request) {
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
		Name     string `json:"name"`
		SQL      string `json:"sql"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	name := strings.ReplaceAll(strings.TrimSpace(req.Name), "`", "")
	sqlStmt := strings.TrimSpace(req.SQL)
	if name == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "name is required")
		return
	}
	if !strings.HasPrefix(strings.ToUpper(sqlStmt), "SUBMIT TASK") {
		stream.WriteJSONError(w, http.StatusForbidden, "only SUBMIT TASK statements are allowed")
		return
	}

	// Drop the old task, then recreate. If recreate fails, the task is gone — the
	// client keeps the edited SQL so the admin can fix and retry. SUBMIT TASK needs
	// a selected database, so the recreate runs with USE set.
	if _, err := db.ExecContext(r.Context(), "DROP TASK IF EXISTS `"+name+"`"); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, "failed to drop existing task: "+err.Error())
		return
	}
	if err := execWithDatabase(r.Context(), db, req.Database, sqlStmt); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, "task dropped, but recreate failed: "+err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Scheduled task updated successfully",
	})
}

// Trigger runs a scheduled task's statement immediately, outside its schedule.
// StarRocks has no first-class "run now" for SUBMIT TASK tasks, so we look up the
// task's DEFINITION from information_schema and execute it directly (same as a
// batch-load INSERT). This runs synchronously and is not recorded in task_runs.
func (h *SchedulerHandler) Trigger(w http.ResponseWriter, r *http.Request) {
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
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "name is required")
		return
	}

	// Look up the task definition server-side rather than trusting client SQL.
	safeName := strings.ReplaceAll(name, "'", "''")
	lookup := "SELECT DEFINITION as Definition FROM information_schema.tasks" +
		" WHERE TASK_NAME = '" + safeName + "' LIMIT 1"
	rows, err := queryToMaps(db, r, lookup)
	if err != nil {
		stream.WriteJSONError(w, http.StatusInternalServerError, "failed to look up task: "+err.Error())
		return
	}
	if len(rows) == 0 {
		stream.WriteJSONError(w, http.StatusNotFound, "task not found: "+name)
		return
	}

	definition := strings.TrimSpace(toString(rows[0]["Definition"]))
	if definition == "" {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, "task has no executable definition")
		return
	}

	// Only allow INSERT-style definitions to be re-run manually.
	upper := strings.ToUpper(definition)
	if !strings.HasPrefix(upper, "INSERT") {
		stream.WriteJSONError(w, http.StatusForbidden,
			"manual trigger only supports INSERT-based tasks")
		return
	}

	if _, err := db.ExecContext(r.Context(), definition); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Task triggered successfully",
	})
}

// Drop deletes a scheduled task by name.
func (h *SchedulerHandler) Drop(w http.ResponseWriter, r *http.Request) {
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
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		stream.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Strip backticks so the name can't break out of the quoted identifier.
	name := strings.ReplaceAll(strings.TrimSpace(req.Name), "`", "")
	if name == "" {
		stream.WriteJSONError(w, http.StatusBadRequest, "name is required")
		return
	}

	sql := "DROP TASK IF EXISTS `" + name + "`"
	if _, err := db.ExecContext(r.Context(), sql); err != nil {
		stream.WriteJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	stream.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Scheduled task dropped successfully",
	})
}

// toString coerces a scanned SQL value (often []byte) to a string.
func toString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}
