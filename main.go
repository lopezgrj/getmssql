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

// parseTables parses the 'tables' subcommand flags.
func parseTables(args []string) error {
	tablesCmd := flag.NewFlagSet("tables", flag.ExitOnError)
	if err := tablesCmd.Parse(args); err != nil {
		return err
	}
	return nil
}

// parseFields parses the 'fields' subcommand flags and validates arguments.
func parseFields(args []string) (string, error) {
	fieldsCmd := flag.NewFlagSet("fields", flag.ExitOnError)
	if err := fieldsCmd.Parse(args); err != nil {
		return "", err
	}
	if fieldsCmd.NArg() < 1 {
		return "", fmt.Errorf("please provide a table name")
	}
	return fieldsCmd.Arg(0), nil
}

// parseDownload parses the 'download' subcommand flags and validates arguments.
func parseDownload(args []string) (table, fieldsFile, format string, err error) {
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)
	downloadFields := downloadCmd.String("fields", "", "File with list of fields to export (one per line)")
	downloadFormat := downloadCmd.String("format", "json", "Output format: json, tsv, csv, sqlite3, duckdb")
	if err := downloadCmd.Parse(args); err != nil {
		return "", "", "", err
	}
	if downloadCmd.NArg() < 1 {
		return "", "", "", fmt.Errorf("please provide a table name")
	}
	return downloadCmd.Arg(0), *downloadFields, *downloadFormat, nil
}

// showUsage prints the usage instructions for the application
func showUsage() {
	fmt.Println(`Usage:
	  go run main.go tables
		List all tables in the database
	  go run main.go fields <table_name>
		List all fields in the specified table
	  go run main.go download [--fields <fields_file>] [--format <format>] <table_name>
		Export data from the specified table. Format can be: json, tsv, csv, sqlite3, duckdb (default: json)`)
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

	missingVars := []string{}
	if server == "" {
		missingVars = append(missingVars, "MSSQL_SERVER")
	}
	if port == "" {
		missingVars = append(missingVars, "MSSQL_PORT")
	}
	if user == "" {
		missingVars = append(missingVars, "MSSQL_USER")
	}
	if password == "" {
		missingVars = append(missingVars, "MSSQL_PASSWORD")
	}
	if database == "" {
		missingVars = append(missingVars, "MSSQL_DATABASE")
	}
	if len(missingVars) > 0 {
		log.Fatalf("Missing required environment variables: %s", strings.Join(missingVars, ", "))
	}

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

	if len(os.Args) < 2 {
		showUsage()
		return
	}

	// Handle -h and --help
	if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
		showUsage()
		return
	}

	switch os.Args[1] {
	case "download":
		table, fieldsFile, format, err := parseDownload(os.Args[2:])
		if err != nil {
			log.Fatalf("%v", err)
		}
		format = strings.ToLower(format)
		asTSV := format == "tsv"
		asCSV := format == "csv"
		asSQLite := format == "sqlite3"
		asDuckDB := format == "duckdb"
		if err := dbexport.DownloadTable(db, table, fieldsFile, asTSV, asCSV, asSQLite, asDuckDB); err != nil {
			log.Fatalf("Error downloading table: %v", err)
		}
		return
	case "tables":
		if err := parseTables(os.Args[2:]); err != nil {
			log.Fatalf("Error parsing tables command: %v", err)
		}
		if err := dbexport.ListTables(db); err != nil {
			log.Fatalf("Error listing tables: %v", err)
		}
		return
	case "fields":
		table, err := parseFields(os.Args[2:])
		if err != nil {
			log.Fatalf("%v", err)
		}
		if err := dbexport.ListFields(db, table); err != nil {
			log.Fatalf("Error listing fields: %v", err)
		}
		return
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		showUsage()
		return
	}
}
