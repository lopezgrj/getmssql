package cli

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	dbexport "getmssql/dbexport"
)

// Flag variables for command line arguments
// These flags are used to specify the database connection parameters.
// They can be set via command line arguments or environment variables.
// setConnectionFlags sets package-level pointers for connection flags for use in env.go
var (
	flagServer   string
	flagPort     string
	flagUser     string
	flagPassword string
	flagDatabase string
)

// RunCLI parses CLI arguments and dispatches commands.
// It returns an error if the command is invalid or if a subcommand fails.
func RunCLI() error {
	// Use a local FlagSet for CLI flags to avoid test conflicts
	fs := flag.NewFlagSet("getmssql", flag.ExitOnError)
	// Add -h and --help support to print usage and all flags
	for _, arg := range os.Args[1:] {
		if arg == "-h" || arg == "--help" {
			showUsage(os.Stderr)
			fmt.Fprintln(os.Stderr, "")
			fs.PrintDefaults()
			os.Exit(0)
		}
	}
	fs.StringVar(&flagServer, "server", "", "MSSQL server hostname or IP (env: MSSQL_SERVER)")
	fs.StringVar(&flagPort, "port", "", "MSSQL server port (env: MSSQL_PORT)")
	fs.StringVar(&flagUser, "user", "", "MSSQL username (env: MSSQL_USER)")
	fs.StringVar(&flagPassword, "password", "", "MSSQL password (env: MSSQL_PASSWORD)")
	fs.StringVar(&flagDatabase, "database", "", "MSSQL database name (env: MSSQL_DATABASE)")
	// Parse command line arguments
	fs.Parse(os.Args[1:])
	args := fs.Args()
	// Check if any command is provided
	if len(args) < 1 {
		showUsage(os.Stderr)
		return fmt.Errorf("no command provided")
	}

	// Handle version command
	switch args[0] {
	case "version":
		fmt.Println("getmssql version", Version)
		return nil
	case "download":
		// Require tablename argument
		if len(args) < 2 || strings.TrimSpace(args[1]) == "" {
			return fmt.Errorf("missing required tablename argument for 'download'. Usage: %s download <tablename> [options]", os.Args[0])
		}
		var runErr error
		runErr = withDB(func(ctx context.Context, db *sql.DB) error {
			table, fieldsFile, format, err := parseDownload(os.Args[2:])
			if err != nil {
				return err
			}
			format = strings.ToLower(format)
			asTSV := format == "tsv"
			asCSV := format == "csv"
			asSQLite := format == "sqlite3"
			asDuckDB := format == "duckdb"
			err = dbexport.DownloadTable(db, table, fieldsFile, asTSV, asCSV, asSQLite, asDuckDB)
			if err != nil {
				if isInvalidTableError(err) {
					return fmt.Errorf("%v.\n\nverifica que el nombre de la tabla o vista exista en la base de datos y esté correctamente escrito. si pertenece a otro esquema, usa el nombre completo (por ejemplo: esquema.tabla)", err)
				}
			}
			return err
		})
		return runErr
	case "tables":
		// tables lists all tables in the database.
		var runErr error
		runErr = withDB(func(ctx context.Context, db *sql.DB) error {
			if err := parseTables(os.Args[2:]); err != nil {
				return fmt.Errorf("error parsing tables command: %v", err)
			}
			return dbexport.ListTables(db)
		})
		return runErr
	case "fields":
		// Require tablename argument
		if len(args) < 2 || strings.TrimSpace(args[1]) == "" {
			return fmt.Errorf("missing required tablename argument for 'fields'. Usage: %s fields <tablename>", os.Args[0])
		}
		var runErr error
		runErr = withDB(func(ctx context.Context, db *sql.DB) error {
			table, err := parseFields(os.Args[2:])
			if err != nil {
				return err
			}
			err = dbexport.ListFields(db, table)
			if err != nil {
				if isInvalidTableError(err) {
					return fmt.Errorf("%v.\n\nverifica que el nombre de la tabla o vista exista en la base de datos y esté correctamente escrito. si pertenece a otro esquema, usa el nombre completo (por ejemplo: esquema.tabla)", err)
				}
			}
			return err
		})
		return runErr
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		showUsage(os.Stderr)
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}
