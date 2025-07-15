
# getmssql

`getmssql` is a simple and idiomatic Go project for exporting tables from a Microsoft SQL Server database to JSON files.

## Features

- List all tables in the database
- List all fields (columns) for a specific table
- Download/export all rows from a table as a formatted JSON file
- Progress messages for downloads, including row count

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
go run main.go download <table_name>
```
Downloads all rows from the specified table and saves them as `<table_name>.json` in the current directory. Shows progress in the console.

## Example

```
$ go run main.go download <TABLE_NAME>
Starting download of table '<TABLE NAME>'...
Downloaded 1000 rows...
Downloaded 2000 rows...
... (progress updates) ...
Total rows downloaded: 5000
Writing data to JSON file...
Table '<TABLE NAME>' data written to <table name>.json in 2.3s
```

## Output

- The downloaded JSON file is formatted for readability.
- The filename is the table name in lowercase, with a `.json` extension.

## License

MIT
