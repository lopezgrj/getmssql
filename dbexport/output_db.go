package dbexport

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// WriteDuckDB writes table data to a DuckDB database file.
func WriteDuckDB(rows *sql.Rows, cols []string, table string, start time.Time) error {
	dbFile := "output.duckdb"
	tableLower := strings.ToLower(table)
	duckdb, err := sql.Open("duckdb", dbFile)
	if err != nil {
		return fmt.Errorf("error opening DuckDB database: %w", err)
	}
	defer duckdb.Close()

	// Check if table exists
	var tableExists int
	err = duckdb.QueryRow(fmt.Sprintf("SELECT count(*) FROM information_schema.tables WHERE table_name='%s'", tableLower)).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("error checking if table exists in DuckDB: %w", err)
	}
	if tableExists > 0 {
		fmt.Printf("Table '%s' already exists in %s. Delete and recreate? (y/N): ", tableLower, dbFile)
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(strings.TrimSpace(response)) == "y" {
			dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableLower)
			if _, err := duckdb.Exec(dropStmt); err != nil {
				return fmt.Errorf("error dropping table in DuckDB: %w", err)
			}
			fmt.Printf("Table '%s' dropped.\n", tableLower)
		} else {
			fmt.Println("Aborted by user.")
			return nil
		}
	}

	// Create table (DuckDB uses double quotes for identifiers)
	colDefs := make([]string, len(cols))
	for i, col := range cols {
		colDefs[i] = fmt.Sprintf("\"%s\" TEXT", col)
	}
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)", tableLower, strings.Join(colDefs, ", "))
	if _, err := duckdb.Exec(createStmt); err != nil {
		return fmt.Errorf("error creating table in DuckDB: %w", err)
	}
	quotedCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCols[i] = fmt.Sprintf("\"%s\"", col)
	}
	insertStmt := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", tableLower, strings.Join(quotedCols, ", "), strings.TrimRight(strings.Repeat("?,", len(cols)), ","))
	batchSize := 10000
	rowCount := 0
	tx, err := duckdb.Begin()
	if err != nil {
		return fmt.Errorf("error starting DuckDB transaction: %w", err)
	}
	stmt, err := tx.Prepare(insertStmt)
	if err != nil {
		return fmt.Errorf("error preparing DuckDB statement: %w", err)
	}
	defer stmt.Close()
	for rows.Next() {
		vals := ScanRowValues(rows, cols)
		if _, err := stmt.Exec(vals...); err != nil {
			return fmt.Errorf("error inserting row into DuckDB: %w", err)
		}
		rowCount++
		if rowCount%batchSize == 0 {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("error committing DuckDB transaction: %w", err)
			}
			tx, err = duckdb.Begin()
			if err != nil {
				return fmt.Errorf("error starting DuckDB transaction: %w", err)
			}
			stmt, err = tx.Prepare(insertStmt)
			if err != nil {
				return fmt.Errorf("error preparing DuckDB statement: %w", err)
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
		return fmt.Errorf("error committing DuckDB transaction: %w", err)
	}
	fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	elapsed := time.Since(start)
	fmt.Printf("Table '%s' data written to %s (table: %s) in %s\n", table, dbFile, table, elapsed)
	return nil
}

// WriteSQLite writes table data to a SQLite3 database file.
func WriteSQLite(rows *sql.Rows, cols []string, table string, start time.Time) error {
	dbFile := "output.sqlite3"
	tableLower := strings.ToLower(table)
	sqliteDB, err := sql.Open("sqlite3", dbFile)
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
		fmt.Scanln(&response)
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
	insertStmt := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES (%s)", tableLower, strings.Join(cols, ", "), strings.TrimRight(strings.Repeat("?,", len(cols)), ","))
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
