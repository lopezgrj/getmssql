package dbexport

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func ListTables(db *sql.DB) error {
	query := `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME`
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying tables: %w", err)
	}
	defer rows.Close()

	fmt.Println("Tables in the database:")
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("error scanning table name: %w", err)
		}
		fmt.Println(tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error: %w", err)
	}
	return nil
}

func ListFields(db *sql.DB, table string) error {
	query := `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1 ORDER BY ORDINAL_POSITION`
	rows, err := db.Query(query, table)
	if err != nil {
		return fmt.Errorf("error querying fields: %w", err)
	}
	defer rows.Close()

	fmt.Printf("Fields in table '%s':\n", table)
	fmt.Println("Column Name\tType\tNullable")
	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return fmt.Errorf("error scanning field: %w", err)
		}
		fmt.Printf("%s\t%s\t%s\n", colName, dataType, isNullable)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error: %w", err)
	}
	return nil
}

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
		fmt.Printf("Warning: could not get total row count: %v\n", err)
		totalRows = -1
	}

	fmt.Printf("Starting download of table '%s'%s... ", table, func() string {
		if fieldsFile != "" {
			return fmt.Sprintf(" with fields from '%s'", fieldsFile)
		} else {
			return ""
		}
	}())
	if totalRows >= 0 {
		fmt.Printf("(total rows: %d)\n", totalRows)
	} else {
		fmt.Printf("(total rows: unknown)\n")
	}

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

func WriteFileOutput(rows *sql.Rows, cols []string, table string, asTSV, asCSV bool, start time.Time) error {
	var filename string
	if asCSV {
		filename = fmt.Sprintf("%s.csv", strings.ToLower(table))
	} else if asTSV {
		filename = fmt.Sprintf("%s.tsv", strings.ToLower(table))
	} else {
		filename = fmt.Sprintf("%s.json", strings.ToLower(table))
	}
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer file.Close()

	rowCount := 0
	if asCSV {
		file.WriteString(strings.Join(cols, "||") + "\n")
	} else if asTSV {
		file.WriteString(strings.Join(cols, "\t") + "\n")
	} else {
		file.WriteString("[")
	}
	first := true
	for rows.Next() {
		rowMap := ScanRowMap(rows, cols)
		if asCSV {
			var rowVals []string
			for _, colName := range cols {
				val := rowMap[colName]
				switch v := val.(type) {
				case nil:
					rowVals = append(rowVals, "")
				case string:
					rowVals = append(rowVals, v)
				default:
					rowVals = append(rowVals, fmt.Sprint(v))
				}
			}
			if _, err := file.WriteString(strings.Join(rowVals, "||") + "\n"); err != nil {
				return fmt.Errorf("error writing CSV row: %w", err)
			}
		} else if asTSV {
			var rowVals []string
			for _, colName := range cols {
				val := rowMap[colName]
				switch v := val.(type) {
				case nil:
					rowVals = append(rowVals, "")
				case string:
					rowVals = append(rowVals, v)
				default:
					rowVals = append(rowVals, fmt.Sprint(v))
				}
			}
			if _, err := file.WriteString(strings.Join(rowVals, "\t") + "\n"); err != nil {
				return fmt.Errorf("error writing TSV row: %w", err)
			}
		} else {
			if !first {
				if _, err := file.WriteString(",\n"); err != nil {
					return fmt.Errorf("error writing JSON separator: %w", err)
				}
			}
			first = false
			encBuf, err := json.MarshalIndent(rowMap, "  ", "  ")
			if err != nil {
				return fmt.Errorf("error marshaling row: %w", err)
			}
			if _, err := file.Write(encBuf); err != nil {
				return fmt.Errorf("error writing JSON row: %w", err)
			}
		}
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("\rDownloaded %d rows...", rowCount)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error: %w", err)
	}
	if asCSV || asTSV {
		fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	} else {
		if _, err := file.WriteString("]\n"); err != nil {
			return fmt.Errorf("error writing JSON close: %w", err)
		}
		fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	}

	elapsed := time.Since(start)
	fmt.Printf("Table '%s' data written to %s in %s\n", table, filename, elapsed)
	return nil
}

func ScanRowValues(rows *sql.Rows, cols []string) []interface{} {
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	if err := rows.Scan(columnPointers...); err != nil {
		panic(fmt.Sprintf("Error scanning row: %v", err))
	}
	vals := make([]interface{}, len(cols))
	for i := range cols {
		val := columnPointers[i].(*interface{})
		v := *val
		switch t := v.(type) {
		case time.Time:
			vals[i] = t.Format("2006-01-02")
		case []uint8:
			s := string(t)
			if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
				vals[i] = intVal
			} else if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
				vals[i] = floatVal
			} else {
				vals[i] = s
			}
		default:
			vals[i] = v
		}
	}
	return vals
}

func ScanRowMap(rows *sql.Rows, cols []string) map[string]interface{} {
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	if err := rows.Scan(columnPointers...); err != nil {
		panic(fmt.Sprintf("Error scanning row: %v", err))
	}
	rowMap := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		v := *val
		switch t := v.(type) {
		case time.Time:
			rowMap[colName] = t.Format("2006-01-02")
		case []uint8:
			s := string(t)
			if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
				rowMap[colName] = intVal
			} else if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
				rowMap[colName] = floatVal
			} else {
				rowMap[colName] = s
			}
		default:
			rowMap[colName] = v
		}
	}
	return rowMap
}
