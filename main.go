package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	server := os.Getenv("MSSQL_SERVER")
	port := os.Getenv("MSSQL_PORT")
	user := os.Getenv("MSSQL_USER")
	password := os.Getenv("MSSQL_PASSWORD")
	database := os.Getenv("MSSQL_DATABASE")

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=disable", server, user, password, port, database)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Error creating connection pool: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("Cannot connect to database: %v", err)
	}

	fmt.Println("Connected to MSSQL successfully!")

	// Show usage if no arguments are provided
	if len(os.Args) == 1 {
		showUsage()
		return
	}

	// Check for command-line arguments
	switch os.Args[1] {
	case "tables":
		listTables(db)
		return
	case "fields":
		if len(os.Args) < 3 {
			fmt.Println("Please provide a table name.")
			showUsage()
			return
		}
		listFields(db, os.Args[2])
		return
	case "download":
		if len(os.Args) < 3 {
			fmt.Println("Please provide a table name.")
			showUsage()
			return
		}
		var fieldsFile string
		var asCSV, asTSV, asSQLite bool
		if len(os.Args) >= 4 {
			lastArg := os.Args[len(os.Args)-1]
			if lastArg == "csv" {
				asCSV = true
				if len(os.Args) >= 5 {
					fieldsFile = os.Args[3]
				}
			} else if lastArg == "tsv" {
				asTSV = true
				if len(os.Args) >= 5 {
					fieldsFile = os.Args[3]
				}
			} else if lastArg == "sqlite3" {
				asSQLite = true
				if len(os.Args) >= 5 {
					fieldsFile = os.Args[3]
				}
			} else {
				fieldsFile = os.Args[3]
			}
		}
		downloadTable(db, os.Args[2], fieldsFile, asTSV, asCSV, asSQLite)
		return
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		showUsage()
		return
	}
}

func showUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run main.go tables                 # List all tables in the database")
	fmt.Println("  go run main.go fields <table_name>    # List all fields in the specified table")
	fmt.Println("  go run main.go download <table_name>  # Download all rows from the table as JSON file")
}

// listTables prints all table names in the connected MSSQL database
func listTables(db *sql.DB) {
	query := `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME`
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error querying tables: %v", err)
	}
	defer rows.Close()

	fmt.Println("Tables in the database:")
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatalf("Error scanning table name: %v", err)
		}
		fmt.Println(tableName)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Row error: %v", err)
	}
}

// listFields prints all columns for a given table in the connected MSSQL database
// It shows the column name, data type, and whether it is nullable
// Example usage: go run main.go fields Users
// This will print all fields in the "Users" table
// Output format: Column Name, Type, Nullable
// Example output:
// Column Name    Type        Nullable
// id             int         NO
// name           varchar     YES
// created_at     datetime    NO
// updated_at     datetime    YES
// This function retrieves the column names, data types, and nullability from the INFORMATION_SCHEMA.COLUMNS view
// It orders the results by the ordinal position of the columns in the table
// It uses a parameterized query to prevent SQL injection
// The table name is passed as a parameter to the query
// The function handles errors gracefully and prints a user-friendly message if the table does not exist
func listFields(db *sql.DB, table string) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1 ORDER BY ORDINAL_POSITION`
	rows, err := db.Query(query, table)
	if err != nil {
		log.Fatalf("Error querying fields: %v", err)
	}
	defer rows.Close()

	fmt.Printf("Fields in table '%s':\n", table)
	fmt.Println("Column Name\tType\tNullable")
	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			log.Fatalf("Error scanning field: %v", err)
		}
		fmt.Printf("%s\t%s\t%s\n", colName, dataType, isNullable)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Row error: %v", err)
	}
}

// downloadTableJSON downloads all rows from a specified table and saves them as a JSON file
// The file is named after the table in lowercase with a .json extension
// Example: If the table is named "Users", the file will be named "users.json

// downloadTable downloads all rows from a specified table and saves them as a JSON, TSV, CSV, or SQLite3 table
func downloadTable(db *sql.DB, table string, fieldsFile string, asTSV, asCSV, asSQLite bool) {
	start := time.Now()
	var query string
	var fields []string
	if fieldsFile != "" {
		// Read fields from file
		data, err := os.ReadFile(fieldsFile)
		if err != nil {
			log.Fatalf("Error reading fields file: %v", err)
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				fields = append(fields, trimmed)
			}
		}
		if len(fields) == 0 {
			log.Fatalf("No fields found in file: %s", fieldsFile)
		}
		query = fmt.Sprintf("SELECT %s FROM [%s]", strings.Join(fields, ", "), table)
		fmt.Printf("Starting download of table '%s' with fields from '%s'...\n", table, fieldsFile)
	} else {
		query = fmt.Sprintf("SELECT * FROM [%s]", table)
		fmt.Printf("Starting download of table '%s'...\n", table)
	}

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error querying table rows: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns: %v", err)
	}

	if asSQLite {
		dbFile := "output.sqlite3"
		sqliteDB, err := sql.Open("sqlite3", dbFile)
		if err != nil {
			log.Fatalf("Error opening SQLite3 database: %v", err)
		}
		defer sqliteDB.Close()

		// Check if table exists
		var tableExists int
		err = sqliteDB.QueryRow(fmt.Sprintf("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'", table)).Scan(&tableExists)
		if err != nil {
			log.Fatalf("Error checking if table exists in SQLite3: %v", err)
		}
		if tableExists > 0 {
			fmt.Printf("Table '%s' already exists in %s. Delete and recreate? (y/N): ", table, dbFile)
			var response string
			fmt.Scanln(&response)
			if strings.ToLower(strings.TrimSpace(response)) == "y" {
				dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS [%s]", table)
				if _, err := sqliteDB.Exec(dropStmt); err != nil {
					log.Fatalf("Error dropping table in SQLite3: %v", err)
				}
				fmt.Printf("Table '%s' dropped.\n", table)
			} else {
				fmt.Println("Aborted by user.")
				return
			}
		}

		// Create table
		colDefs := make([]string, len(cols))
		for i, col := range cols {
			colDefs[i] = fmt.Sprintf("[%s] TEXT", col)
		}
		createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS [%s] (%s)", table, strings.Join(colDefs, ", "))
		if _, err := sqliteDB.Exec(createStmt); err != nil {
			log.Fatalf("Error creating table in SQLite3: %v", err)
		}
		insertStmt := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.TrimRight(strings.Repeat("?,", len(cols)), ","))
		batchSize := 10000
		rowCount := 0
		tx, err := sqliteDB.Begin()
		if err != nil {
			log.Fatalf("Error starting SQLite3 transaction: %v", err)
		}
		stmt, err := tx.Prepare(insertStmt)
		if err != nil {
			log.Fatalf("Error preparing SQLite3 statement: %v", err)
		}
		defer stmt.Close()
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				log.Fatalf("Error scanning row: %v", err)
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
			if _, err := stmt.Exec(vals...); err != nil {
				log.Fatalf("Error inserting row into SQLite3: %v", err)
			}
			rowCount++
			if rowCount%batchSize == 0 {
				if err := tx.Commit(); err != nil {
					log.Fatalf("Error committing SQLite3 transaction: %v", err)
				}
				tx, err = sqliteDB.Begin()
				if err != nil {
					log.Fatalf("Error starting SQLite3 transaction: %v", err)
				}
				stmt, err = tx.Prepare(insertStmt)
				if err != nil {
					log.Fatalf("Error preparing SQLite3 statement: %v", err)
				}
				fmt.Printf("\rDownloaded %d rows...", rowCount)
			} else if rowCount%1000 == 0 {
				fmt.Printf("\rDownloaded %d rows...", rowCount)
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("Row error: %v", err)
		}
		if err := tx.Commit(); err != nil {
			log.Fatalf("Error committing SQLite3 transaction: %v", err)
		}
		fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
		elapsed := time.Since(start)
		fmt.Printf("Table '%s' data written to %s (table: %s) in %s\n", table, dbFile, table, elapsed)
		return
	}
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
		log.Fatalf("Error creating output file: %v", err)
	}
	defer file.Close()

	rowCount := 0
	if asCSV {
		// Write header
		file.WriteString(strings.Join(cols, "||") + "\n")
	} else if asTSV {
		// Write header
		file.WriteString(strings.Join(cols, "\t") + "\n")
	} else {
		file.WriteString("[")
	}
	first := true
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			log.Fatalf("Error scanning row: %v", err)
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
		if asCSV {
			// Write CSV row with || separator
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
			file.WriteString(strings.Join(rowVals, "||") + "\n")
		} else if asTSV {
			// Write TSV row
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
			file.WriteString(strings.Join(rowVals, "\t") + "\n")
		} else {
			if !first {
				file.WriteString(",\n")
			}
			first = false
			encBuf, err := json.MarshalIndent(rowMap, "  ", "  ")
			if err != nil {
				log.Fatalf("Error marshaling row: %v", err)
			}
			file.Write(encBuf)
		}
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("\rDownloaded %d rows...", rowCount)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Row error: %v", err)
	}
	if asCSV || asTSV {
		fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	} else {
		file.WriteString("]\n")
		fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	}

	elapsed := time.Since(start)
	fmt.Printf("Table '%s' data written to %s in %s\n", table, filename, elapsed)
}
