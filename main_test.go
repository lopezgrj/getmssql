package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"getmssql/dbexport"
	"getmssql/cli"

	"github.com/DATA-DOG/go-sqlmock"
)

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
	// Optionally, parse fields from the query string if you want to check them
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
	mock.ExpectQuery("SELECT a, b, c FROM test").WillReturnRows(
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
	mock.ExpectQuery("SELECT a, b FROM test").WillReturnRows(
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

func TestCLI_HelpFlag(t *testing.T) {
	// Save and restore os.Args and os.Stdout
	origArgs := os.Args
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	os.Args = []string{"getmssql", "-h"}

	defer func() {
		os.Args = origArgs
		os.Stdout = origStdout
	}()

	// main() should call usage and exit, so recover panic
	defer func() {
		_ = recover()
	}()

	main()
	w.Close()
	out, _ := io.ReadAll(r)
	if !strings.Contains(string(out), "Usage:") {
		t.Errorf("expected usage output, got: %s", string(out))
	}
}

func TestCLI_InvalidFlag(t *testing.T) {
	origArgs := os.Args
	rErr, wErr, _ := os.Pipe()
	rOut, wOut, _ := os.Pipe()
	origStderr := os.Stderr
	origStdout := os.Stdout
	os.Stderr = wErr
	os.Stdout = wOut

	os.Args = []string{"getmssql", "-notaflag"}

	defer func() {
		os.Args = origArgs
		os.Stderr = origStderr
		os.Stdout = origStdout
	}()

	err := cli.RunCLI()
	// Give the flag package a moment to flush output
	time.Sleep(10 * time.Millisecond)
	_ = wErr.Sync()
	_ = wOut.Sync()
	wErr.Close()
	wOut.Close()
	errOut, _ := io.ReadAll(rErr)
	stdOut, _ := io.ReadAll(rOut)
	os.Stderr = origStderr
	os.Stdout = origStdout
	outStr := string(errOut) + string(stdOut)
	if err == nil {
		t.Errorf("expected error for invalid flag, got nil")
	}
	if !strings.Contains(outStr, "flag provided but not defined") && !strings.Contains(outStr, "unknown flag") && !strings.Contains(outStr, "Unknown command:") {
		t.Errorf("expected flag error, got: %s", outStr)
	}
}
