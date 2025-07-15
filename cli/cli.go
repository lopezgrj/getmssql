package cli

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	dbexport "getmssql/dbexport"

	"github.com/joho/godotenv"
)

// withDB handles DB connection, context, signal handling, and cleanup. It calls fn with the DB and context.
func withDB(fn func(ctx context.Context, db *sql.DB) error) {
	server, port, user, password, database := loadAndValidateEnv()
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=disable", server, user, password, port, database)
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Error creating connection pool: %v", err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		select {
		case sig := <-interrupted:
			fmt.Fprintf(os.Stderr, "\nReceived %v. Closing database connection and exiting...\n", sig)
			db.Close()
			os.Exit(130)
		default:
		}
	}()
	if err := db.Ping(); err != nil {
		log.Fatalf("Cannot connect to database: %v", err)
	}
	fmt.Println("Connected to MSSQL successfully!")
	if err := fn(ctx, db); err != nil {
		log.Fatalf("%v", err)
	}
}

// showUsage prints the usage instructions for the application to the given writer.
func showUsage(w *os.File) {
	fmt.Fprint(w, `Usage:
getmssql tables
  List all tables in the database

getmssql fields <table_name>
  List all fields in the specified table

getmssql download [--fields <fields_file>] [--format <format>] <table_name>
  Export data from the specified table.
  Format can be: json, tsv, csv, sqlite3, duckdb (default: json)`)
}

// loadAndValidateEnv loads .env and required MSSQL environment variables, returning them or logging fatally if missing.
func loadAndValidateEnv() (server, port, user, password, database string) {
	_ = godotenv.Load()
	server = strings.TrimSpace(os.Getenv("MSSQL_SERVER"))
	port = strings.TrimSpace(os.Getenv("MSSQL_PORT"))
	user = strings.TrimSpace(os.Getenv("MSSQL_USER"))
	password = strings.TrimSpace(os.Getenv("MSSQL_PASSWORD"))
	database = strings.TrimSpace(os.Getenv("MSSQL_DATABASE"))
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
	return
}

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

// RunCLI parses CLI arguments and dispatches commands.
func RunCLI() {
	if len(os.Args) < 2 {
		showUsage(os.Stderr)
		os.Exit(1)
	}

	// Handle -h and --help
	if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
		showUsage(os.Stdout)
		return
	}

	switch os.Args[1] {
	case "download":
		withDB(func(ctx context.Context, db *sql.DB) error {
			table, fieldsFile, format, err := parseDownload(os.Args[2:])
			if err != nil {
				return err
			}
			format = strings.ToLower(format)
			asTSV := format == "tsv"
			asCSV := format == "csv"
			asSQLite := format == "sqlite3"
			asDuckDB := format == "duckdb"
			return dbexport.DownloadTable(db, table, fieldsFile, asTSV, asCSV, asSQLite, asDuckDB)
		})
		return
	case "tables":
		withDB(func(ctx context.Context, db *sql.DB) error {
			if err := parseTables(os.Args[2:]); err != nil {
				return fmt.Errorf("error parsing tables command: %v", err)
			}
			return dbexport.ListTables(db)
		})
		return
	case "fields":
		withDB(func(ctx context.Context, db *sql.DB) error {
			table, err := parseFields(os.Args[2:])
			if err != nil {
				return err
			}
			return dbexport.ListFields(db, table)
		})
		return
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		showUsage(os.Stderr)
		os.Exit(1)
	}
}
