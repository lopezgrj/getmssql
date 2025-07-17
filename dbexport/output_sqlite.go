package dbexport

import (
	"fmt"
	"strings"
	"time"
)

// WriteSQLite writes table data to a SQLite3 database file.
func WriteSQLite(rows Rows, columns []string, table string, now time.Time) error {
	return WriteSQLiteWithDeps(rows, columns, table, now)
}

// WriteSQLiteWithDeps writes table data to a SQLite3 database file (for testability).
func WriteSQLiteWithDeps(rows Rows, cols []string, table string, start time.Time) error {
	if rows == nil {
		return fmt.Errorf("rows is nil")
	}
	dbFile := "output.sqlite3"
	tableLower := strings.ToLower(table)
	sqliteDB, err := openSQLite("sqlite3", dbFile)
	if err != nil {
		return fmt.Errorf("error opening SQLite3 database: %w", err)
	}
	defer sqliteDB.Close()

	// Check if table exists
	var tableExists int
	err = sqliteDB.QueryRow(fmt.Sprintf("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'", tableLower)).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("error checking if table exists in SQLite3: %w", err)
	}
	if tableExists > 0 {
		fmt.Printf("Table '%s' already exists in %s. Delete and recreate? (y/N): ", tableLower, dbFile)
		var response string
		scanln(&response)
		if strings.ToLower(strings.TrimSpace(response)) == "y" {
			dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS [%s]", tableLower)
			if _, err := sqliteDB.Exec(dropStmt); err != nil {
				return fmt.Errorf("error dropping table in SQLite3: %w", err)
			}
			fmt.Printf("Table '%s' dropped.\n", tableLower)
		} else {
			fmt.Println("Aborted by user.")
			return nil
		}
	}

	// Create table
	colDefs := make([]string, len(cols))
	for i, col := range cols {
		colDefs[i] = fmt.Sprintf("[%s] TEXT", col)
	}
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS [%s] (%s)", tableLower, strings.Join(colDefs, ", "))
	if _, err := sqliteDB.Exec(createStmt); err != nil {
		return fmt.Errorf("error creating table in SQLite3: %w", err)
	}
	// Quote column names for safety
	quotedCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCols[i] = fmt.Sprintf("[%s]", col)
	}
	insertStmt := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES (%s)", tableLower, strings.Join(quotedCols, ", "), strings.TrimRight(strings.Repeat("?,", len(cols)), ","))
	batchSize := 10000
	rowCount := 0
	tx, err := sqliteDB.Begin()
	if err != nil {
		return fmt.Errorf("error starting SQLite3 transaction: %w", err)
	}
	stmt, err := tx.Prepare(insertStmt)
	if err != nil {
		return fmt.Errorf("error preparing SQLite3 statement: %w", err)
	}
	defer stmt.Close()
	for rows.Next() {
		vals := ScanRowValues(rows, cols)
		if _, err := stmt.Exec(vals...); err != nil {
			return fmt.Errorf("error inserting row into SQLite3: %w", err)
		}
		rowCount++
		if rowCount%batchSize == 0 {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("error committing SQLite3 transaction: %w", err)
			}
			tx, err = sqliteDB.Begin()
			if err != nil {
				return fmt.Errorf("error starting SQLite3 transaction: %w", err)
			}
			stmt, err = tx.Prepare(insertStmt)
			if err != nil {
				return fmt.Errorf("error preparing SQLite3 statement: %w", err)
			}
			fmt.Printf("\rDownloaded %d rows...", rowCount)
		} else if rowCount%1000 == 0 {
			fmt.Printf("\rDownloaded %d rows...", rowCount)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing SQLite3 transaction: %w", err)
	}
	fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	elapsed := time.Since(start)
	fmt.Printf("Table '%s' data written to %s (table: %s) in %s\n", table, dbFile, table, elapsed)
	return nil
}
