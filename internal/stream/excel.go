package stream

import (
	"database/sql"
	"fmt"
	"io"

	"github.com/xuri/excelize/v2"
)

// ExportExcel streams query results as an XLSX file using excelize's StreamWriter.
func ExportExcel(w io.Writer, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting columns: %w", err)
	}

	f := excelize.NewFile()
	defer f.Close()

	sheetName := "Sheet1"
	sw, err := f.NewStreamWriter(sheetName)
	if err != nil {
		return fmt.Errorf("creating stream writer: %w", err)
	}

	// Write header row
	header := make([]interface{}, len(columns))
	for i, col := range columns {
		header[i] = col
	}
	cell, _ := excelize.CoordinatesToCellName(1, 1)
	if err := sw.SetRow(cell, header); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	// Prepare scan destinations
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rowNum := 2
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		rowData := make([]interface{}, len(columns))
		for i, val := range values {
			if b, ok := val.([]byte); ok {
				rowData[i] = string(b)
			} else if val == nil {
				rowData[i] = ""
			} else {
				rowData[i] = val
			}
		}

		cell, _ := excelize.CoordinatesToCellName(1, rowNum)
		if err := sw.SetRow(cell, rowData); err != nil {
			return err
		}
		rowNum++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if err := sw.Flush(); err != nil {
		return fmt.Errorf("flushing stream writer: %w", err)
	}

	return f.Write(w)
}
