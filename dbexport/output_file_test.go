package dbexport

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestWriteFileOutput_CSV_TSV_JSON(t *testing.T) {
	columns := []string{"a", "b"}
	// Use a real in-memory SQLite DB to create *sql.Rows for data
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE sometable (a TEXT, b INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO sometable (a, b) VALUES ('foo', 1), ('bar', 2)")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	types := []struct {
		asCSV bool
		asTSV bool
		ext   string
	}{
		{true, false, ".csv"},
		{false, true, ".tsv"},
		{false, false, ".json"},
	}

	for _, typ := range types {
		sqlRows, err := db.Query("SELECT a, b FROM sometable")
		if err != nil {
			t.Fatalf("failed to create sql.Rows: %v", err)
		}
		filename := "sometable" + typ.ext
		os.Remove(filename)
		err = WriteFileOutput(sqlRows, columns, "sometable", typ.asTSV, typ.asCSV, time.Now())
		sqlRows.Close()
		if err != nil {
			t.Errorf("WriteFileOutput failed for %s: %v", typ.ext, err)
		}
		if _, err := os.Stat(filename); err != nil {
			t.Errorf("expected file %s to be created", filename)
		} else {
			os.Remove(filename)
		}
	}
}

func TestWriteFileOutput_Error(t *testing.T) {
	// Should error if file cannot be created (simulate by using invalid filename)
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE sometable (a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	sqlRows, err := db.Query("SELECT a FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	defer sqlRows.Close()
	// Use a filename with a slash to force error
	err = WriteFileOutput(sqlRows, []string{"a"}, "/invalid/name", false, true, time.Now())
	if err == nil {
		t.Errorf("expected error for invalid filename, got nil")
	}
}

// Renamed to avoid redeclaration conflict
func TestWriteFileOutput_EdgeCases_Local(t *testing.T) {
	columns := []string{"a", "b"}
	// Use a real in-memory SQLite DB to create *sql.Rows for data
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite3: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE sometable (a TEXT, b INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	// No rows case
	sqlRows, err := db.Query("SELECT a, b FROM sometable WHERE 1=0")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	filename := "sometable.csv"
	os.Remove(filename)
	err = WriteFileOutput(sqlRows, columns, "sometable", false, true, time.Now())
	sqlRows.Close()
	if err != nil {
		t.Errorf("WriteFileOutput failed for empty rows: %v", err)
	}
	if _, err := os.Stat(filename); err != nil {
		t.Errorf("expected file %s to be created for empty rows", filename)
	} else {
		os.Remove(filename)
	}

	// Large row count for progress print
	for i := 0; i < 2001; i++ {
		_, err = db.Exec("INSERT INTO sometable (a, b) VALUES (?, ?)", fmt.Sprintf("row%d", i), i)
		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}
	}
	sqlRows, err = db.Query("SELECT a, b FROM sometable")
	if err != nil {
		t.Fatalf("failed to create sql.Rows: %v", err)
	}
	filename = "sometable.tsv"
	os.Remove(filename)
	err = WriteFileOutput(sqlRows, columns, "sometable", true, false, time.Now())
	sqlRows.Close()
	if err != nil {
		t.Errorf("WriteFileOutput failed for large row count: %v", err)
	}
	if _, err := os.Stat(filename); err != nil {
		t.Errorf("expected file %s to be created for large row count", filename)
	} else {
		os.Remove(filename)
	}
}
