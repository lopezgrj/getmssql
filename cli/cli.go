package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	dbexport "getmssql/dbexport"
)

// RunCLI parses CLI arguments and dispatches commands.
// It returns an error if the command is invalid or if a subcommand fails.
func RunCLI() error {
	if len(os.Args) < 2 {
		showUsage(os.Stderr)
		return fmt.Errorf("no command provided")
	}

	// Handle -h and --help
	if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
		showUsage(os.Stdout)
		return nil
	}

	switch os.Args[1] {
	case "version":
		fmt.Println("getmssql version", Version)
		return nil
	case "download":
		// download exports data from the specified table in the chosen format.
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
		// fields lists all fields in the specified table.
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
