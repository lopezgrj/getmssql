package dbexport

import "database/sql"

var openSQLite func(driver, dsn string) (*sql.DB, error)
var openDuckDB func(driver, dsn string) (*sql.DB, error)
var scanln func(a ...interface{}) (int, error)
