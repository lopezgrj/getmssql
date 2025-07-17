

![Go CI](https://github.com/lopezgrj/getmssql/actions/workflows/ci.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![codecov](https://codecov.io/gh/lopezrj/getmssql/branch/main/graph/badge.svg)](https://codecov.io/gh/lopezrj/getmssql)

# getmssql

`getmssql` is a simple and idiomatic Go project for exporting tables from a Microsoft SQL Server database to various formats (JSON, TSV, CSV, SQLite3, Duckdb).

## Features

- List all tables in the database
- List all fields (columns) for a specific table
- Download/export all rows from a table as JSON, TSV, CSV, SQLite3, or DuckDB
- Select specific fields to export using a text file
- Progress messages for downloads, including row count
- Efficient streaming and batching for large tables
- SQLite3 and DuckDB output: prompts before overwriting existing tables

## Prerequisites

- Go 1.23 or newer
- Access to a Microsoft SQL Server instance

## Setup

1. Clone this repository.
2. Create a `.env` file in the project root with your MSSQL connection details:

   ```env
   MSSQL_SERVER=your_server
   MSSQL_PORT=1433
   MSSQL_USER=your_username
   MSSQL_PASSWORD=your_password
   MSSQL_DATABASE=your_database
   ```

3. Install dependencies:

   ```sh
   go mod tidy
   ```

## Usage

Run the program with one of the following commands:

```
go run main.go tables
```
Lists all tables in the database.

```
go run main.go fields <table_name>
```
Lists all fields (columns) in the specified table.

```
go run main.go download [--fields=fields.txt] [--format=json|tsv|csv|sqlite3|duckdb] <table_name>
```
Downloads all rows from the specified table in the chosen format. Default is JSON. Shows progress in the console.

**Flags:**
- `--fields=fields.txt` : (optional) File with list of fields to export (one per line)
- `--format=json|tsv|csv|sqlite3|duckdb` : (optional) Output format (default: json)

## Examples

### Example: Download as JSON
```
$ go run main.go download mytable
Starting download of table 'mytable'...
Downloaded 1000 rows...
Downloaded 2000 rows...
... (progress updates) ...
Total rows downloaded: 5000
Table 'mytable' data written to mytable.json in 2.3s
```

### Example: Download as CSV with selected fields
```
$ go run main.go download --fields=fields.txt --format=csv mytable
Starting download of table 'mytable' with fields from 'fields.txt'...
Downloaded 1000 rows...
... (progress updates) ...
Table 'mytable' data written to mytable.csv in 1.8s
```


### Example: Download to SQLite3
```
$ go run main.go download --format=sqlite3 mytable
Table 'mytable' already exists in output.sqlite3. Delete and recreate? (y/N): y
Table 'mytable' dropped.
Downloaded 10000 rows...
... (progress updates) ...
Table 'mytable' data written to output.sqlite3 (table: mytable) in 4.2s
```

### Example: Download to DuckDB
```
$ go run main.go download --format=duckdb mytable
Table 'mytable' already exists in output.duckdb. Delete and recreate? (y/N): y
Table 'mytable' dropped.
Downloaded 10000 rows...
... (progress updates) ...
Table 'mytable' data written to output.duckdb (table: mytable) in 4.2s
```

## Output

- Output file is named after the table (e.g., `mytable.json`, `mytable.csv`, `mytable.tsv`, `output.sqlite3` for SQLite3, or `output.duckdb` for DuckDB)
- JSON output is formatted for readability
- SQLite3 and DuckDB output create or overwrite a table in their respective databases (with confirmation)

## License

MIT
