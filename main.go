// getmssql is a CLI tool for exporting data from a Microsoft SQL Server database.
//
// Usage:
//
//	go run main.go tables
//	  List all tables in the database
//	go run main.go fields <table_name>
//	  List all fields in the specified table
//	go run main.go download [--fields <fields_file>] [--format <format>] <table_name>
//	  Export data from the specified table. Format can be: json, tsv, csv, sqlite3, duckdb (default: json)
package main

import (
	"getmssql/cli"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	cli.RunCLI()
}
