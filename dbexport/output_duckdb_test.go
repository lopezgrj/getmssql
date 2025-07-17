package dbexport

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestWriteDuckDBWithDeps_Errors2(t *testing.T) {
	columns := []string{"a"}
	now := time.Now()

	// openDB returns error
	err := WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) {
		return nil, fmt.Errorf("open error")
	}, scanln)
	if err == nil || !strings.Contains(err.Error(), "open error") {
		t.Errorf("expected open error, got: %v", err)
	}

	// error checking if table exists
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnError(fmt.Errorf("exists error"))
	err = WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) { return db, nil }, scanln)
	db.Close()
	if err == nil || !strings.Contains(err.Error(), "exists error") {
		t.Errorf("expected exists error, got: %v", err)
	}

	// user aborts at overwrite prompt
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectExec("DROP TABLE IF EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	scanlnAbort := func(a ...interface{}) (int, error) {
		if len(a) > 0 {
			if ptr, ok := a[0].(*string); ok {
				*ptr = "n"
			}
		}
		return 1, nil
	}
	err = WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) { return db, nil }, scanlnAbort)
	db.Close()
	if err != nil {
		t.Errorf("expected nil for user abort, got: %v", err)
	}

	// error dropping table
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	scanlnYes := func(a ...interface{}) (int, error) {
		if len(a) > 0 {
			if ptr, ok := a[0].(*string); ok {
				*ptr = "y"
			}
		}
		return 1, nil
	}
	mock.ExpectExec("DROP TABLE IF EXISTS").WillReturnError(fmt.Errorf("drop error"))
	err = WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) { return db, nil }, scanlnYes)
	db.Close()
	if err == nil || !strings.Contains(err.Error(), "drop error") {
		t.Errorf("expected drop error, got: %v", err)
	}

	// error creating table
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(fmt.Errorf("create error"))
	err = WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) { return db, nil }, scanln)
	db.Close()
	if err == nil || !strings.Contains(err.Error(), "create error") {
		t.Errorf("expected create error, got: %v", err)
	}

	// error starting transaction
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db2.Close()
	mock2.ExpectBegin().WillReturnError(fmt.Errorf("begin error"))
	err = WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) { return db2, nil }, scanln)
	if err == nil || !strings.Contains(err.Error(), "begin error") {
		t.Errorf("expected begin error, got: %v", err)
	}

	// error preparing statement
	db3, mock3, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db3.Close()
	mock3.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock3.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	mock3.ExpectBegin()
	mock3.ExpectPrepare("INSERT INTO").WillReturnError(fmt.Errorf("prepare error"))
	err = WriteDuckDBWithDeps(&sql.Rows{}, columns, "table", now, func(string, string) (*sql.DB, error) { return db3, nil }, scanln)
	if err == nil || !strings.Contains(err.Error(), "prepare error") {
		t.Errorf("expected prepare error, got: %v", err)
	}

	// error inserting row (simulate with sqlmock row error)
	db4, mock4, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db4.Close()
	mock4.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock4.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	mock4.ExpectBegin()
	mock4.ExpectPrepare("INSERT INTO").ExpectExec().WillReturnError(fmt.Errorf("insert error"))
	// Use a real *sql.Rows with no rows to trigger Exec error
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
	err = WriteDuckDBWithDeps(sqlRows, columns, "table", now, func(string, string) (*sql.DB, error) { return db4, nil }, scanln)
	if err == nil || !strings.Contains(err.Error(), "insert error") {
		t.Errorf("expected insert error, got: %v", err)
	}

	// error committing transaction (final commit)
	db5, mock5, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db5.Close()
	mock5.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables WHERE table_name='table'").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock5.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	mock5.ExpectBegin()
	mock5.ExpectPrepare("INSERT INTO").ExpectExec().WithArgs("foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock5.ExpectCommit().WillReturnError(fmt.Errorf("commit error"))
	// Use a real *sql.Rows with one row
	sqlite2, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite2.Close()
	_, err = sqlite2.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = sqlite2.Exec("INSERT INTO sometable (a) VALUES ('foo')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	sqlRows2, err := sqlite2.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows2.Close()
	err = WriteDuckDBWithDeps(sqlRows2, columns, "table", now, func(string, string) (*sql.DB, error) { return db5, nil }, scanln)
	if err == nil || !strings.Contains(err.Error(), "commit error") {
		t.Errorf("expected commit error, got: %v", err)
	}

	// error from rows.Err() (simulate by closing rows early)
	db6, mock6, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db6.Close()
	mock6.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.tables WHERE table_name='table'").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock6.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	mock6.ExpectBegin()
	mock6.ExpectPrepare("INSERT INTO").ExpectExec().WithArgs("foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock6.ExpectCommit()
	sqlite3, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite3.Close()
	_, err = sqlite3.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = sqlite3.Exec("INSERT INTO sometable (a) VALUES ('foo')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	sqlRows3, err := sqlite3.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	sqlRows3.Close() // force error on rows.Err()
	err = WriteDuckDBWithDeps(sqlRows3, columns, "table", now, func(string, string) (*sql.DB, error) { return db6, nil }, scanln)
	if err == nil {
		t.Errorf("expected error from rows.Err, got: %v", err)
	}
}

func TestWriteDuckDB_ErrorAndSuccess2(t *testing.T) {
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

func TestWriteDuckDBRows_Minimal(t *testing.T) {
	// Error path: nil rows
	err := WriteDuckDBRows(nil, []string{"a"}, "table", time.Now())
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

	err = WriteDuckDBRows(sqlRows, columns, "table", time.Now())
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}
}

func TestWriteDuckDBRows_BatchInsert(t *testing.T) {
	// This test inserts >10,000 rows to exercise batch commit logic
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a"}

	// Expect table check, create, begin, prepare, and multiple exec/commit for batches
	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables WHERE table_name='table'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 1))
	// For 10,001 rows, expect 2 commits (10,000 + 1)
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`)
	for i := 0; i < 10000; i++ {
		mock.ExpectExec(`INSERT INTO`).WithArgs(fmt.Sprintf("row%d", i)).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
	// Final batch (1 row)
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`)
	mock.ExpectExec(`INSERT INTO`).WithArgs("row10000").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Use a real SQLite DB to generate *sql.Rows with 10,001 rows
	sqlite, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite.Close()
	_, err = sqlite.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for i := 0; i < 10001; i++ {
		_, err = sqlite.Exec("INSERT INTO sometable (a) VALUES (?)", fmt.Sprintf("row%d", i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}
	sqlRows, err := sqlite.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows.Close()

	// Patch openDuckDB and scanln for the wrapper
	origOpenDuckDB := openDuckDB
	origScanln := scanln
	openDuckDB = func(driver, dsn string) (*sql.DB, error) {
		return db, nil
	}
	scanln = func(a ...interface{}) (int, error) { return 1, nil }
	defer func() { openDuckDB = origOpenDuckDB; scanln = origScanln }()

	err = WriteDuckDBRows(sqlRows, columns, "table", time.Now())
	if err != nil {
		t.Errorf("expected success for >10,000 rows, got: %v", err)
	}
}

func TestWriteDuckDBRows_BatchCommitError(t *testing.T) {
	// Simulate error on tx.Commit() after a batch
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a"}

	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables WHERE table_name='table'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`)
	for i := 0; i < 10000; i++ {
		mock.ExpectExec(`INSERT INTO`).WithArgs(fmt.Sprintf("row%d", i)).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit().WillReturnError(fmt.Errorf("batch commit error"))

	// Use a real SQLite DB to generate *sql.Rows with 10,000 rows
	sqlite, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite.Close()
	_, err = sqlite.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for i := 0; i < 10000; i++ {
		_, err = sqlite.Exec("INSERT INTO sometable (a) VALUES (?)", fmt.Sprintf("row%d", i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}
	sqlRows, err := sqlite.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows.Close()

	origOpenDuckDB := openDuckDB
	origScanln := scanln
	openDuckDB = func(driver, dsn string) (*sql.DB, error) {
		return db, nil
	}
	scanln = func(a ...interface{}) (int, error) { return 1, nil }
	defer func() { openDuckDB = origOpenDuckDB; scanln = origScanln }()

	err = WriteDuckDBRows(sqlRows, columns, "table", time.Now())
	if err == nil || !strings.Contains(err.Error(), "batch commit error") {
		t.Errorf("expected batch commit error, got: %v", err)
	}
}

func TestWriteDuckDBRows_BatchPrepareError(t *testing.T) {
	// Simulate error on tx.Prepare() after a batch
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a"}

	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables WHERE table_name='table'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`)
	for i := 0; i < 10000; i++ {
		mock.ExpectExec(`INSERT INTO`).WithArgs(fmt.Sprintf("row%d", i)).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
	// Next batch: begin, prepare fails
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`).WillReturnError(fmt.Errorf("batch prepare error"))

	// Use a real SQLite DB to generate *sql.Rows with 10,001 rows
	sqlite, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer sqlite.Close()
	_, err = sqlite.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for i := 0; i < 10001; i++ {
		_, err = sqlite.Exec("INSERT INTO sometable (a) VALUES (?)", fmt.Sprintf("row%d", i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}
	sqlRows, err := sqlite.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows.Close()

	origOpenDuckDB := openDuckDB
	origScanln := scanln
	openDuckDB = func(driver, dsn string) (*sql.DB, error) {
		return db, nil
	}
	scanln = func(a ...interface{}) (int, error) { return 1, nil }
	defer func() { openDuckDB = origOpenDuckDB; scanln = origScanln }()

	err = WriteDuckDBRows(sqlRows, columns, "table", time.Now())
	if err == nil || !strings.Contains(err.Error(), "batch prepare error") {
		t.Errorf("expected batch prepare error, got: %v", err)
	}
}

func TestWriteDuckDBRows_RowsErr(t *testing.T) {
	// Simulate error on rows.Err() after iteration
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a"}

	mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables WHERE table_name='table'`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectPrepare(`INSERT INTO`).ExpectExec().WithArgs("foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Use a real SQLite DB to generate *sql.Rows, but close early to simulate rows.Err
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
	sqlRows.Close() // force error on rows.Err()

	origOpenDuckDB := openDuckDB
	origScanln := scanln
	openDuckDB = func(driver, dsn string) (*sql.DB, error) {
		return db, nil
	}
	scanln = func(a ...interface{}) (int, error) { return 1, nil }
	defer func() { openDuckDB = origOpenDuckDB; scanln = origScanln }()

	err = WriteDuckDBRows(sqlRows, columns, "table", time.Now())
	if err == nil {
		t.Errorf("expected error from rows.Err, got: %v", err)
	}
}
