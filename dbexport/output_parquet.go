package dbexport

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// WriteParquet exports rows to a Parquet file.
func WriteParquet(rows *sql.Rows, columns []string, filePath string, now time.Time) error {
	if rows == nil {
		return fmt.Errorf("rows is nil")
	}
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided")
	}

	// Open output file
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer fw.Close()

	// Use a generic map for each row
	pw, err := writer.NewJSONWriter("root", fw, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer pw.WriteStop()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}
	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))

	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(valPtrs...); err != nil {
			return fmt.Errorf("scan error: %w", err)
		}
		rowMap := map[string]interface{}{}
		for i, col := range cols {
			rowMap[col] = vals[i]
		}
		if err := pw.Write(rowMap); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}
	return nil
}
