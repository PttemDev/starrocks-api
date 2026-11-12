package stream

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// QueryResultJSON streams query results as NDJSON (newline-delimited JSON).
// Each line is a JSON object representing one row.
// First line is metadata: {"columns": [...], "types": [...]}
// Subsequent lines are data rows: {"col1": val1, "col2": val2, ...}
func QueryResultJSON(w http.ResponseWriter, rows *sql.Rows) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("response writer does not support flushing")
	}

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting columns: %w", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("getting column types: %w", err)
	}

	types := make([]string, len(colTypes))
	for i, ct := range colTypes {
		types[i] = ct.DatabaseTypeName()
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)

	// Write metadata line
	meta := map[string]interface{}{
		"type":    "metadata",
		"columns": columns,
		"types":   types,
	}
	if err := enc.Encode(meta); err != nil {
		return err
	}
	flusher.Flush()

	// Prepare scan destinations
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			// Write error as a special line
			errLine := map[string]interface{}{
				"type":  "error",
				"error": err.Error(),
				"row":   rowCount,
			}
			enc.Encode(errLine)
			flusher.Flush()
			return err
		}

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for JSON encoding
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		if err := enc.Encode(row); err != nil {
			return err
		}

		rowCount++
		// Flush every 100 rows for efficiency
		if rowCount%100 == 0 {
			flusher.Flush()
		}
	}

	// Final flush
	flusher.Flush()

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// QueryResultBuffered collects all rows into a single JSON response.
// Returns {"columns": [...], "types": [...], "rows": [{...}, ...]}
// Suitable for B2B consumers who prefer standard JSON over NDJSON streaming.
func QueryResultBuffered(w http.ResponseWriter, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting columns: %w", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("getting column types: %w", err)
	}

	types := make([]string, len(colTypes))
	for i, ct := range colTypes {
		types[i] = ct.DatabaseTypeName()
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var data []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scanning row: %w", err)
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
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if data == nil {
		data = []map[string]interface{}{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(map[string]interface{}{
		"columns": columns,
		"types":   types,
		"rows":    data,
	})
}

// WriteJSONError writes a JSON error response.
func WriteJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// WriteJSON writes a single JSON object response.
func WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ExportJSON streams query results as a JSON array using streaming writes.
func ExportJSON(w io.Writer, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	w.Write([]byte("[\n"))
	enc := json.NewEncoder(w)
	first := true

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		if !first {
			w.Write([]byte(",\n"))
		}
		first = false

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		if err := enc.Encode(row); err != nil {
			return err
		}

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}

	w.Write([]byte("]\n"))

	return rows.Err()
}
