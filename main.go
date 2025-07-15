package main

import (
	"database/sql"
	"flag"
	"fmt"
	dbexport "getmssql/dbexport"
	"log"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/joho/godotenv"
	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
)

// showUsage prints the usage instructions for the application
func showUsage() {
	fmt.Println(`Usage:
  go run main.go tables
	List all tables in the database
  go run main.go fields <table_name>
	List all fields in the specified table
  go run main.go download [--fields <fields_file>] [--format <format>] <table_name>
	Export data from the specified table. Format can be: json, tsv, csv, sqlite3, duckdb (default: json)
`)
}

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
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

	if err := db.Ping(); err != nil {
		log.Fatalf("Cannot connect to database: %v", err)
	}

	fmt.Println("Connected to MSSQL successfully!")

	// Subcommands
	tablesCmd := flag.NewFlagSet("tables", flag.ExitOnError)
	fieldsCmd := flag.NewFlagSet("fields", flag.ExitOnError)
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)

	// Download flags
	downloadFields := downloadCmd.String("fields", "", "File with list of fields to export (one per line)")
	downloadFormat := downloadCmd.String("format", "json", "Output format: json, tsv, csv, sqlite3, duckdb")

	if len(os.Args) < 2 {
		showUsage()
		return
	}

	switch os.Args[1] {
	case "tables":
		tablesCmd.Parse(os.Args[2:])
		if err := dbexport.ListTables(db); err != nil {
			log.Fatalf("Error listing tables: %v", err)
		}
		return
	case "fields":
		fieldsCmd.Parse(os.Args[2:])
		if fieldsCmd.NArg() < 1 {
			fmt.Println("Please provide a table name.")
			showUsage()
			return
			log.Fatalf("Error listing fields: %v", err)
		}
		return
	case "download":
		downloadCmd.Parse(os.Args[2:])
		if downloadCmd.NArg() < 1 {
			fmt.Println("Please provide a table name.")
			showUsage()
			return
		}
		table := downloadCmd.Arg(0)
		fieldsFile := *downloadFields
		format := strings.ToLower(*downloadFormat)
		asTSV := format == "tsv"
		asCSV := format == "csv"
		asSQLite := format == "sqlite3"
		asDuckDB := format == "duckdb"
		if err := dbexport.DownloadTable(db, table, fieldsFile, asTSV, asCSV, asSQLite, asDuckDB); err != nil {
			log.Fatalf("Error downloading table: %v", err)
		}
		return
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		showUsage()
		return
	}
}
