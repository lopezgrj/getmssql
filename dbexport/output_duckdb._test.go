package dbexport

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestWriteDuckDB_ErrorAndSuccess(t *testing.T) {
	// Error path: nil rows should error
	err := WriteDuckDBWithDeps(nil, []string{"a"}, "table", time.Now(), nil, nil)
	if err == nil {
		t.Errorf("expected error for nil rows, got nil")
	}

	// Success path: use sqlmock for DuckDB and real SQLite for data rows
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a"}

	openDuck := func(driver, dsn string) (*sql.DB, error) {
		return db, nil
	}
	scanlnStub := func(a ...interface{}) (int, error) { return 1, nil }

	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables WHERE table_name='table'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`).ExpectExec().WithArgs("foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Use a real in-memory SQLite DB to create *sql.Rows for data
	sqlite, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite.Close()
	_, err = sqlite.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = sqlite.Exec("INSERT INTO sometable (a) VALUES ('foo')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	sqlRows, err := sqlite.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows.Close()

	err = WriteDuckDBWithDeps(sqlRows, columns, "table", time.Now(), openDuck, scanlnStub)
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}
}

func TestWriteDuckDB_WrapperCoverage(t *testing.T) {
	// Error path: nil rows
	err := WriteDuckDB(nil, []string{"a"}, "table", time.Now())
	if err == nil {
		t.Errorf("expected error for nil rows, got nil")
	}

	// Success path: use sqlmock for DuckDB and real SQLite for data rows
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a"}

	origOpenDuckDB := openDuckDB
	origScanln := scanln
	openDuckDB = func(driver, dsn string) (*sql.DB, error) {
		return db, nil
	}
	scanln = func(a ...interface{}) (int, error) { return 1, nil }
	defer func() { openDuckDB = origOpenDuckDB; scanln = origScanln }()

	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables WHERE table_name='table'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`).ExpectExec().WithArgs("foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Use a real in-memory SQLite DB to create *sql.Rows for data
	sqlite, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite.Close()
	_, err = sqlite.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = sqlite.Exec("INSERT INTO sometable (a) VALUES ('foo')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	sqlRows, err := sqlite.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows.Close()

	err = WriteDuckDB(sqlRows, columns, "table", time.Now())
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}
}
