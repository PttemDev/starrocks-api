package stream

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
)

// ExportCSV streams query results as CSV, flushing periodically.
func ExportCSV(w io.Writer, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting columns: %w", err)
	}

	cw := csv.NewWriter(w)

	// Write header
	if err := cw.Write(columns); err != nil {
		return fmt.Errorf("writing csv header: %w", err)
	}
	cw.Flush()
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	record := make([]string, len(columns))
	rowCount := 0

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		for i, val := range values {
			record[i] = formatValue(val)
		}

		if err := cw.Write(record); err != nil {
			return err
		}

		rowCount++
		if rowCount%100 == 0 {
			cw.Flush()
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}

	cw.Flush()
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	if err := cw.Error(); err != nil {
		return err
	}
	return rows.Err()
}

func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}
