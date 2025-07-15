// listTables prints all table names in the connected MSSQL database
// listFields prints all columns for a given table in the connected MSSQL database

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

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
		downloadTableJSON(db, os.Args[2])
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

func downloadTableJSON(db *sql.DB, table string) {
	start := time.Now()
	fmt.Printf("Starting download of table '%s'...\n", table)
	query := fmt.Sprintf("SELECT * FROM [%s]", table)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error querying table rows: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns: %v", err)
	}

	var results []map[string]interface{}
	rowCount := 0
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
			rowMap[colName] = *val
		}
		results = append(results, rowMap)
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("\rDownloaded %d rows...", rowCount)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Row error: %v", err)
	}
	fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)

	fmt.Printf("Writing data to JSON file...\n")
	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	filename := fmt.Sprintf("%s.json", strings.ToLower(table))
	if err := os.WriteFile(filename, jsonData, 0644); err != nil {
		log.Fatalf("Error writing JSON file: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf("Table '%s' data written to %s in %s\n", table, filename, elapsed)
}
