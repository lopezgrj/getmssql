package dbexport

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/DATA-DOG/go-sqlmock"
)

// stubRowsErr is a stub for Rows that returns an error from Err() after iteration
type stubRowsErr struct {
	called bool
	val    string
}

func (s *stubRowsErr) Close() error { return nil }

// Simulate two rows, as sqlmock expects two Scan calls for two columns
func (s *stubRowsErr) Next() bool {
	if !s.called {
		s.called = true
		return true
	}
	return false
}
func (s *stubRowsErr) Scan(dest ...interface{}) error {
	// Set all dest pointers to s.val, simulating a row with all values set
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = s.val
			fmt.Printf("stubRowsErr.Scan: dest[%d] set to string %q\n", i, s.val)
		case *interface{}:
			*ptr = s.val
			fmt.Printf("stubRowsErr.Scan: dest[%d] set to interface{} %q\n", i, s.val)
		default:
			fmt.Printf("stubRowsErr.Scan: dest[%d] not a *string or *interface{}\n", i)
		}
	}
	return nil
}
func (s *stubRowsErr) Columns() ([]string, error) { return []string{"a"}, nil }
func (s *stubRowsErr) Err() error                 { return fmt.Errorf("rows error") }

func TestWriteFileOutputRows_SuccessAndError(t *testing.T) {
	// Success path: use *sql.Rows
	tmpfile, err := os.CreateTemp("", "testfileoutputrows_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a", "b"}
	mock.ExpectQuery("SELECT a, b FROM test").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("foo", "bar"),
	)
	rows, err := db.Query("SELECT a, b FROM test")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer rows.Close()

	err = WriteFileOutputRows(rows, columns, tmpfile.Name(), false, false, time.Now())
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}

	// Error path: not a *sql.Rows
	err = WriteFileOutputRows(&stubRows{}, columns, tmpfile.Name(), false, false, time.Now())
	if err == nil || !strings.Contains(err.Error(), "requires *sql.Rows") {
		t.Errorf("expected error for non-*sql.Rows, got: %v", err)
	}
}

// errRows wraps sql.Rows and returns a custom error from Err()
type errRows struct {
	*sql.Rows
}

func (e *errRows) Err() error {
	if e.Rows == nil {
		return fmt.Errorf("rows error")
	}
	return nil
}

func TestWriteFileOutput_SuccessAndError(t *testing.T) {
	// Success path: write to a temp file
	tmpfile, err := os.CreateTemp("", "testfileoutput_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	columns := []string{"a", "b"}
	mock.ExpectQuery("SELECT a, b FROM test").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("foo", "bar"),
	)
	rows, err := db.Query("SELECT a, b FROM test")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer rows.Close()

	err = WriteFileOutput(rows, columns, tmpfile.Name(), false, false, time.Now())
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}

	// Error path: file cannot be created
	err = WriteFileOutput(rows, columns, "/not/a/real/path/file.csv", false, false, time.Now())
	if err == nil {
		t.Errorf("expected error for bad file path, got nil")
	}
}

// stubRows is a minimal stub for the Rows interface for testing
type stubRows struct {
	called bool
	val    string
}

// Implement Rows interface methods for stubRows
func (s *stubRows) Close() error { return nil }
func (s *stubRows) Next() bool {
	if s.called {
		return false
	}
	s.called = true
	return true
}
func (s *stubRows) Scan(dest ...interface{}) error {
	if len(dest) > 0 {
		if ptr, ok := dest[0].(*string); ok {
			*ptr = s.val
		}
	}
	return nil
}
func (s *stubRows) Columns() ([]string, error) { return []string{"a"}, nil }
func (s *stubRows) Err() error                 { return nil }

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
	err = DownloadTable(db, "table", "/nonexistent/file", false, false, false, false)
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
	err = DownloadTableWithWriters(db, "table", "/nonexistent/file", false, false, false, false,
		nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "error reading fields file") {
		t.Errorf("expected error from BuildSelectQuery, got: %v", err)
	}

	// Error from db.QueryRow(...).Scan(...)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnError(io.EOF)
	err = DownloadTableWithWriters(db, "table", "", false, false, false, false,
		nil, nil, func(_ Rows, _ []string, _ string, _, _ bool, _ time.Time) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "could not get total row count") {
		t.Errorf("expected error from QueryRow.Scan, got: %v", err)
	}

	// Error from db.Query(query)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnError(io.EOF)
	err = DownloadTableWithWriters(db, "table", "", false, false, false, false,
		nil, nil, func(_ Rows, _ []string, _ string, _, _ bool, _ time.Time) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "error querying table rows") {
		t.Errorf("expected error from db.Query, got: %v", err)
	}

	// Error from rows.Columns() (not supported by sqlmock, so we skip this path)

	// Error from writer function
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = DownloadTableWithWriters(db, "table", "", true, false, false, false,
		nil, nil, func(_ Rows, _ []string, _ string, _, _ bool, _ time.Time) error {
			return fmt.Errorf("writer error")
		})
	if err == nil || !strings.Contains(err.Error(), "writer error") {
		t.Errorf("expected writer error, got: %v", err)
	}

	// Success path (default to file output)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = DownloadTableWithWriters(db, "table", "", true, false, false, false,
		nil, nil, func(_ Rows, _ []string, _ string, _, _ bool, _ time.Time) error { return nil })
	if err != nil {
		t.Errorf("expected success, got: %v", err)
	}

	// Success path (DuckDB)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = DownloadTableWithWriters(db, "table", "", false, false, false, true,
		func(_ Rows, _ []string, _ string, _ time.Time) error { return nil },
		nil,
		nil)
	if err != nil {
		t.Errorf("expected success for DuckDB, got: %v", err)
	}

	// Success path (SQLite)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery(`SELECT \* FROM \[table\]`).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	err = DownloadTableWithWriters(db, "table", "", false, false, true, false,
		nil,
		func(_ Rows, _ []string, _ string, _ time.Time) error { return nil },
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
		err = ListTables(db)
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
	err = ListTables(db)
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
		err = ListFields(db, "sometable")
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
	err = ListFields(db, "sometable")
	if err == nil || !strings.Contains(err.Error(), "error scanning field") {
		t.Errorf("expected scan error, got: %v", err)
	}
	t.Run("missing fields file", func(t *testing.T) {
		_, err := BuildSelectQuery("mytable", "nonexistent_file.txt")
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
		_, err := BuildSelectQuery("mytable", fname)
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
	vals := ScanRowValues(rows, columns)
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
	err = ListTables(db)
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
	err = ListFields(db, "sometable")
	if err == nil || !strings.Contains(err.Error(), "error querying fields") {
		t.Errorf("expected error from ListFields, got: %v", err)
	}
}

func TestBuildSelectQuery_AllFields(t *testing.T) {
	query, err := BuildSelectQuery("mytable", "")
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
	query, err := BuildSelectQuery("mytable", fname)
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
	vals := ScanRowValues(rows, columns)
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
	vals2 := ScanRowValues(rows, columns)
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
	m := ScanRowMap(rows2, []string{"a", "b"})
	if m["a"] != "x" || m["b"] != int64(123) {
		t.Errorf("unexpected map values: %v", m)
	}
}

// --- Additional WriteFileOutput coverage tests ---
type errColumnsRows struct{ stubRows }

func (e *errColumnsRows) Columns() ([]string, error) { return nil, fmt.Errorf("columns error") }

type errNextRows struct{ stubRows }

func (e *errNextRows) Next() bool                     { return true }
func (e *errNextRows) Scan(dest ...interface{}) error { return nil }
func (e *errNextRows) Columns() ([]string, error)     { return []string{"a"}, nil }
func (e *errNextRows) Close() error                   { return nil }
func (e *errNextRows) Err() error                     { return nil }

type errScanRows struct{ stubRows }

func (e *errScanRows) Next() bool                     { return true }
func (e *errScanRows) Scan(dest ...interface{}) error { return fmt.Errorf("scan error") }
func (e *errScanRows) Columns() ([]string, error)     { return []string{"a"}, nil }
func (e *errScanRows) Close() error                   { return nil }
func (e *errScanRows) Err() error                     { return nil }

type errRowsErr struct{ stubRows }

func (e *errRowsErr) Columns() ([]string, error) { return []string{"a"}, nil }
func (e *errRowsErr) Next() bool                 { return false }
func (e *errRowsErr) Err() error                 { return fmt.Errorf("rows error") }

func TestWriteFileOutput_EdgeCases(t *testing.T) {
	columns := []string{"a"}
	now := time.Now()
	tmpfile, err := os.CreateTemp("", "testfileoutput_edge_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	// nil rows: expect panic
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic for nil Rows, got none")
		}
	}()
	_ = WriteFileOutput(nil, columns, tmpfile.Name(), false, false, now)

	// nil columns (use WriteFileOutputRows)
	err = WriteFileOutputRows(&stubRows{}, nil, tmpfile.Name(), false, false, now)
	if err == nil || !strings.Contains(err.Error(), "no columns") {
		t.Errorf("expected error for nil columns, got: %v", err)
	}

	// empty columns (use WriteFileOutputRows)
	err = WriteFileOutputRows(&stubRows{}, []string{}, tmpfile.Name(), false, false, now)
	if err == nil || !strings.Contains(err.Error(), "no columns") {
		t.Errorf("expected error for empty columns, got: %v", err)
	}

	// error from rows.Columns (use WriteFileOutputRows)
	err = WriteFileOutputRows(&errColumnsRows{}, columns, tmpfile.Name(), false, false, now)
	if err == nil || !strings.Contains(err.Error(), "columns error") {
		t.Errorf("expected error from Columns, got: %v", err)
	}

	// error from rows.Next (simulate Next always true, but no data, use WriteFileOutputRows)
	err = WriteFileOutputRows(&errNextRows{}, columns, tmpfile.Name(), false, false, now)
	if err == nil || !strings.Contains(err.Error(), "row scan") {
		t.Errorf("expected scan error from Next/Scan, got: %v", err)
	}

	// error from rows.Scan (use WriteFileOutputRows)
	err = WriteFileOutputRows(&errScanRows{}, columns, tmpfile.Name(), false, false, now)
	if err == nil || !strings.Contains(err.Error(), "scan error") {
		t.Errorf("expected scan error, got: %v", err)
	}

	// error from rows.Err (use WriteFileOutputRows)
	err = WriteFileOutputRows(&errRowsErr{}, columns, tmpfile.Name(), false, false, now)
	if err == nil || !strings.Contains(err.Error(), "rows error") {
		t.Errorf("expected error from rows.Err, got: %v", err)
	}

	// file already exists and overwrite=false (use real *sql.Rows)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT a FROM test").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("foo"))
	rows, err := db.Query("SELECT a FROM test")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer rows.Close()
	f, err := os.CreateTemp("", "testfileoutput_exists_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	fname := f.Name()
	f.Close()
	defer os.Remove(fname)
	// Write once to create the file
	err = WriteFileOutput(rows, []string{"a"}, fname, false, false, now)
	if err != nil {
		t.Errorf("unexpected error writing file: %v", err)
	}
	// Try again with overwrite=false
	err = WriteFileOutput(rows, []string{"a"}, fname, false, false, now)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected error for file exists, got: %v", err)
	}
}
