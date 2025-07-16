package cli

import (
	"fmt"
	"os"
)

// showUsage prints the usage instructions for the application to the given writer.
func showUsage(w *os.File) {
	fmt.Fprint(w, `Usage:
    getmssql [flags] <command> [command options]

  Flags (can also be set via environment variables):
    --server     MSSQL server hostname or IP (env: MSSQL_SERVER)
    --port       MSSQL server port (env: MSSQL_PORT)
    --user       MSSQL username (env: MSSQL_USER)
    --password   MSSQL password (env: MSSQL_PASSWORD)
    --database   MSSQL database name (env: MSSQL_DATABASE)

  Commands:
    tables
      List all tables in the database

    fields <table_name>
      List all fields in the specified table

    download [--fields <fields_file>] [--format <format>] <table_name>
      Export data from the specified table.
      Format can be: json, tsv, csv, sqlite3, duckdb (default: json)`)
}
