package dbexport

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

// DownloadTable exports a table to the selected format using the appropriate writer.
func DownloadTable(db *sql.DB, table string, fieldsFile string, asTSV, asCSV, asSQLite, asDuckDB bool) error {
	start := time.Now()
	query, err := BuildSelectQuery(table, fieldsFile)
	if err != nil {
		return err
	}

	// Get total row count
	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM [%s]", table)
	err = db.QueryRow(countQuery).Scan(&totalRows)
	if err != nil {
		return fmt.Errorf("could not get total row count: %w", err)
	}

	fmt.Printf("Starting download of table '%s'%s... ", table, func() string {
		if fieldsFile != "" {
			return fmt.Sprintf(" with fields from '%s'", fieldsFile)
		} else {
			return ""
		}
	}())
	fmt.Printf("(total rows: %d)\n", totalRows)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying table rows: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error getting columns: %w", err)
	}

	var writeErr error
	switch {
	case asDuckDB:
		writeErr = WriteDuckDB(rows, cols, table, start)
	case asSQLite:
		writeErr = WriteSQLite(rows, cols, table, start)
	default:
		writeErr = WriteFileOutput(rows, cols, table, asTSV, asCSV, start)
	}
	if writeErr != nil {
		return writeErr
	}
	return nil
}

// BuildSelectQuery builds a SELECT query for the given table and optional fields file.
func BuildSelectQuery(table, fieldsFile string) (string, error) {
	if fieldsFile != "" {
		data, err := os.ReadFile(fieldsFile)
		if err != nil {
			return "", fmt.Errorf("error reading fields file: %w", err)
		}
		var fields []string
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				fields = append(fields, trimmed)
			}
		}
		if len(fields) == 0 {
			return "", fmt.Errorf("no fields found in file: %s", fieldsFile)
		}
		return fmt.Sprintf("SELECT %s FROM [%s]", strings.Join(fields, ", "), table), nil
	}
	return fmt.Sprintf("SELECT * FROM [%s]", table), nil
}
