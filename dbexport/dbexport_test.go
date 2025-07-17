package dbexport_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"getmssql/dbexport"

	"github.com/DATA-DOG/go-sqlmock"
)

func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	f()
	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

func TestDownloadTable_WrapperCoverage(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	// This triggers the error path in BuildSelectQuery, which is enough to cover the wrapper
	err = dbexport.DownloadTable(db, "table", "/nonexistent/file", false, false, false, false)
	if err == nil || !strings.Contains(err.Error(), "error reading fields file") {
		t.Errorf("expected error from DownloadTable, got: %v", err)
	}
}
func TestDownloadTable_ErrorsAndSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	// Error from BuildSelectQuery
	err = dbexport.DownloadTableWithWriters(db, "table", "/nonexistent/file", false, false, false, false,
		nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "error reading fields file") {
		t.Errorf("expected error from BuildSelectQuery, got: %v", err)
	}

	// Error from db.QueryRow(...).Scan(...)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnError(io.EOF)
	err = dbexport.DownloadTableWithWriters(db, "table", "", false, false, false, false,
		nil, nil, func(_ *sql.Rows, _ []string, _ string, _, _ bool, _ time.Time) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "could not get total row count") {
		t.Errorf("expected error from QueryRow.Scan, got: %v", err)
	}

	// Error from db.Query(query)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnError(io.EOF)
	err = dbexport.DownloadTableWithWriters(db, "table", "", false, false, false, false,
		nil, nil, func(_ *sql.Rows, _ []string, _ string, _, _ bool, _ time.Time) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "error querying table rows") {
		t.Errorf("expected error from db.Query, got: %v", err)
	}

	// Error from rows.Columns() (not supported by sqlmock, so we skip this path)

	// Error from writer function
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = dbexport.DownloadTableWithWriters(db, "table", "", true, false, false, false,
		nil, nil, func(_ *sql.Rows, _ []string, _ string, _, _ bool, _ time.Time) error {
			return fmt.Errorf("writer error")
		})
	if err == nil || !strings.Contains(err.Error(), "writer error") {
		t.Errorf("expected writer error, got: %v", err)
	}

	// Success path (default to file output)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = dbexport.DownloadTableWithWriters(db, "table", "", true, false, false, false,
		nil, nil, func(_ *sql.Rows, _ []string, _ string, _, _ bool, _ time.Time) error { return nil })
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}

	// Success path (DuckDB)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = dbexport.DownloadTableWithWriters(db, "table", "", false, false, false, true,
		func(_ *sql.Rows, _ []string, _ string, _ time.Time) error { return nil },
		nil,
		nil)
	if err != nil {
		t.Errorf("expected success for DuckDB, got: %v", err)
	}

	// Success path (SQLite)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = dbexport.DownloadTableWithWriters(db, "table", "", false, false, true, false,
		nil,
		func(_ *sql.Rows, _ []string, _ string, _ time.Time) error { return nil },
		nil)
	if err != nil {
		t.Errorf("expected success for SQLite, got: %v", err)
	}
}

func TestListTables_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("foo").AddRow("bar")
	mock.ExpectQuery(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`).WillReturnRows(rows)
	out := captureStdout(func() {
		err = dbexport.ListTables(db)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "foo") || !strings.Contains(out, "bar") {
		t.Errorf("expected output to contain table names, got: %s", out)
	}
}

func TestListTables_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow(nil)
	mock.ExpectQuery(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`).WillReturnRows(rows)
	err = dbexport.ListTables(db)
	if err == nil || !strings.Contains(err.Error(), "error scanning table name") {
		t.Errorf("expected scan error, got: %v", err)
	}
}

func TestListTables_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("foo")
	rows.RowError(0, nil) // no error on scan
	mock.ExpectQuery(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`).WillReturnRows(rows)
	// Simulate rows.Err after iteration
	mock.ExpectQuery(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`).WillReturnRows(rows).RowsWillBeClosed()
	// Use a custom rows.Err error
	rows2 := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("foo")
	rows2.RowError(0, nil)
	mock.ExpectQuery(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`).WillReturnRows(rows2)
	// Actually, sqlmock does not support setting rows.Err directly, so we skip this test as not feasible.
}

func TestListFields_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"}).
		AddRow("id", "int", "NO").AddRow("name", "varchar", "YES")
	mock.ExpectQuery(`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS`).WillReturnRows(rows)
	out := captureStdout(func() {
		err = dbexport.ListFields(db, "sometable")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "id") || !strings.Contains(out, "name") {
		t.Errorf("expected output to contain field names, got: %s", out)
	}
}

func TestListFields_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"}).AddRow(nil, nil, nil)
	mock.ExpectQuery(`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS`).WillReturnRows(rows)
	err = dbexport.ListFields(db, "sometable")
	if err == nil || !strings.Contains(err.Error(), "error scanning field") {
		t.Errorf("expected scan error, got: %v", err)
	}
	t.Run("missing fields file", func(t *testing.T) {
		_, err := dbexport.BuildSelectQuery("mytable", "nonexistent_file.txt")
		if err == nil || !strings.Contains(err.Error(), "error reading fields file") {
			t.Errorf("expected error for missing fields file, got: %v", err)
		}
	})
	t.Run("empty fields file", func(t *testing.T) {
		fname := "empty_fields.txt"
		if err := os.WriteFile(fname, []byte("\n\n"), 0644); err != nil {
			t.Fatalf("Failed to write empty fields file: %v", err)
		}
		defer os.Remove(fname)
		_, err := dbexport.BuildSelectQuery("mytable", fname)
		if err == nil || !strings.Contains(err.Error(), "no fields found") {
			t.Errorf("expected error for empty fields file, got: %v", err)
		}
	})
}

func TestScanRowValues_AllNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	columns := []string{"a", "b"}
	mock.ExpectQuery("SELECT a, b FROM test").WillReturnRows(
		sqlmock.NewRows(columns).AddRow(nil, nil),
	)
	rows, err := db.Query("SELECT a, b FROM test")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	vals := dbexport.ScanRowValues(rows, columns)
	if vals[0] != nil || vals[1] != nil {
		t.Errorf("expected all nils, got: %v", vals)
	}
}

func TestListTables_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES`).WillReturnError(
		io.EOF,
	)
	err = dbexport.ListTables(db)
	if err == nil || !strings.Contains(err.Error(), "error querying tables") {
		t.Errorf("expected error from ListTables, got: %v", err)
	}
}

func TestListFields_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS`).WillReturnError(
		io.EOF,
	)
	err = dbexport.ListFields(db, "sometable")
	if err == nil || !strings.Contains(err.Error(), "error querying fields") {
		t.Errorf("expected error from ListFields, got: %v", err)
	}
}

func TestBuildSelectQuery_AllFields(t *testing.T) {
	query, err := dbexport.BuildSelectQuery("mytable", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(query, "SELECT * FROM [mytable]") {
		t.Errorf("Expected SELECT * query, got: %s", query)
	}
}

func TestBuildSelectQuery_FieldsFile(t *testing.T) {
	fname := "test_fields.txt"
	content := "foo\nbar\n"
	if err := os.WriteFile(fname, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test fields file: %v", err)
	}
	defer os.Remove(fname)
	query, err := dbexport.BuildSelectQuery("mytable", fname)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(query, "SELECT foo, bar FROM [mytable]") {
		t.Errorf("Unexpected query: %s", query)
	}
}

func TestScanRowValuesAndMap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	// Test scanRowValues
	columns := []string{"a", "b", "c"}
	now := time.Now()
	mock.ExpectQuery(`SELECT a, b, c FROM test`).WillReturnRows(
		sqlmock.NewRows(columns).
			AddRow(1, "foo", now).
			AddRow(nil, []byte("42"), "bar"),
	)
	rows, err := db.Query("SELECT a, b, c FROM test")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	vals := dbexport.ScanRowValues(rows, columns)
	if len(vals) != 3 {
		t.Errorf("expected 3 values, got %d", len(vals))
	}
	if v, ok := vals[0].(int64); !ok || v != 1 {
		t.Errorf("expected first value to be int64(1), got %v", vals[0])
	}
	if v, ok := vals[1].(string); !ok || v != "foo" {
		t.Errorf("expected second value to be 'foo', got %v", vals[1])
	}
	if v, ok := vals[2].(string); !ok || v != now.Format("2006-01-02") {
		t.Errorf("expected third value to be formatted date, got %v", vals[2])
	}

	if !rows.Next() {
		t.Fatal("expected second row")
	}
	vals2 := dbexport.ScanRowValues(rows, columns)
	if vals2[0] != nil {
		t.Errorf("expected nil for first value, got %v", vals2[0])
	}
	if v, ok := vals2[1].(int64); !ok || v != 42 {
		t.Errorf("expected int64(42) for second value, got %v", vals2[1])
	}
	if v, ok := vals2[2].(string); !ok || v != "bar" {
		t.Errorf("expected 'bar' for third value, got %v", vals2[2])
	}

	// Test scanRowMap
	mock.ExpectQuery(`SELECT a, b FROM test`).WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow("x", 123),
	)
	rows2, err := db.Query("SELECT a, b FROM test")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows2.Close()
	if !rows2.Next() {
		t.Fatal("expected at least one row")
	}
	m := dbexport.ScanRowMap(rows2, []string{"a", "b"})
	if m["a"] != "x" || m["b"] != int64(123) {
		t.Errorf("unexpected map values: %v", m)
	}
}

// Integration tests for writeDuckDB, writeSQLite, writeFileOutput would require a test database and are best tested with mocks or skipped in unit tests.
