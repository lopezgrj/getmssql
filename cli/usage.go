package cli

import (
	"fmt"
	"os"
)

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
