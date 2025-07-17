package dbexport

import (
	   "context"
	   "database/sql"
	   "os"
	   "strings"
	   "testing"
	   "time"

	   "github.com/DATA-DOG/go-sqlmock"
)

func TestReadFieldsFile(t *testing.T) {
	file := "test_fields.txt"
	content := "col1\ncol2\ncol3\n"
	os.WriteFile(file, []byte(content), 0644)
	t.Cleanup(func() { os.Remove(file) })

	cols, err := ReadFieldsFile(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 3 || cols[0] != "col1" || cols[2] != "col3" {
		t.Errorf("unexpected columns: %v", cols)
	}

	_, err = ReadFieldsFile("nonexistent.txt")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestDownloadTableParquet_FileExists(t *testing.T) {
	mockDB, _, _ := sqlmock.New()
	file := "test.parquet"
	os.WriteFile(file, []byte("dummy"), 0644)
	t.Cleanup(func() { os.Remove(file) })

	err := DownloadTableParquet(mockDB, "sometable", "test_fields.txt")
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected file exists error, got: %v", err)
	}
}

func TestDownloadTableParquet_DBError(t *testing.T) {
	mockDB, mock, _ := sqlmock.New()
	mock.ExpectQuery("SELECT col1 FROM sometable").WillReturnError(sql.ErrConnDone)
	os.WriteFile("test_fields.txt", []byte("col1\n"), 0644)
	t.Cleanup(func() { os.Remove("test_fields.txt") })

	err := DownloadTableParquet(mockDB, "sometable", "test_fields.txt")
	if err == nil || !strings.Contains(err.Error(), "query error") {
		t.Errorf("expected query error, got: %v", err)
	}
}

func TestWriteParquet_Simple(t *testing.T) {
	// Create mock rows
	mockDB, mock, _ := sqlmock.New()
	cols := []string{"col1", "col2"}
	rows := sqlmock.NewRows(cols).
		AddRow("a", "b").
		AddRow("c", "d")
	mock.ExpectQuery("SELECT col1, col2 FROM sometable").WillReturnRows(rows)

	query := "SELECT col1, col2 FROM sometable"
	resultRows, err := mockDB.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("mock query error: %v", err)
	}
	file := "test_out.parquet"
	t.Cleanup(func() { os.Remove(file) })

	err = WriteParquet(resultRows, cols, file, time.Now())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if _, err := os.Stat(file); err != nil {
		t.Errorf("output file not created: %v", err)
	}
}
