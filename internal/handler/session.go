package handler

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

var validSessionVarName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// queryWithSessionVars executes a query on a dedicated connection after applying
// SET commands for the given session variables. When vars is empty it falls back
// to db.QueryContext without acquiring a dedicated connection.
//
// The returned cleanup function MUST be deferred after rows.Close().
func queryWithSessionVars(ctx context.Context, db *sql.DB, query string, vars map[string]string) (*sql.Rows, func(), error) {
	if len(vars) == 0 {
		rows, err := db.QueryContext(ctx, query)
		return rows, func() {}, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	for name, value := range vars {
		if !validSessionVarName.MatchString(name) {
			conn.Close()
			return nil, nil, fmt.Errorf("invalid session variable name: %s", name)
		}
		escaped := strings.ReplaceAll(value, "'", "\\'")
		stmt := fmt.Sprintf("SET %s = '%s'", name, escaped)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("SET %s failed: %w", name, err)
		}
	}

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return rows, func() { conn.Close() }, nil
}
