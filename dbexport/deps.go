package dbexport

import (
	"database/sql"
	"fmt"
)

var openSQLite = func(driver, dsn string) (*sql.DB, error) {
	return sql.Open(driver, dsn)
}
var openDuckDB = func(driver, dsn string) (*sql.DB, error) {
	return sql.Open(driver, dsn)
}
var scanln = func(a ...interface{}) (int, error) {
	return fmt.Scanln(a...)
}
