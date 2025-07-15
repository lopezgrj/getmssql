package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestBuildSelectQuery_AllFields(t *testing.T) {
	query, fields := buildSelectQuery("mytable", "")
	if !strings.HasPrefix(query, "SELECT * FROM [mytable]") {
		t.Errorf("Expected SELECT * query, got: %s", query)
	}
	if fields != nil {
		t.Errorf("Expected nil fields, got: %v", fields)
	}
}

func TestBuildSelectQuery_FieldsFile(t *testing.T) {
	fname := "test_fields.txt"
	content := "foo\nbar\n"
	if err := os.WriteFile(fname, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test fields file: %v", err)
	}
	defer os.Remove(fname)
	query, fields := buildSelectQuery("mytable", fname)
	if !strings.HasPrefix(query, "SELECT foo, bar FROM [mytable]") {
		t.Errorf("Unexpected query: %s", query)
	}
	if len(fields) != 2 || fields[0] != "foo" || fields[1] != "bar" {
		t.Errorf("Unexpected fields: %v", fields)
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
	vals := scanRowValues(rows, columns)
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
	vals2 := scanRowValues(rows, columns)
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
	m := scanRowMap(rows2, []string{"a", "b"})
	if m["a"] != "x" || m["b"] != int64(123) {
		t.Errorf("unexpected map values: %v", m)
	}
}

// Integration tests for writeDuckDB, writeSQLite, writeFileOutput would require a test database and are best tested with mocks or skipped in unit tests.
