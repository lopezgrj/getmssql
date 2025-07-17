package cmd

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// mockFn is a helper to test the callback logic.
func mockFn(ctx context.Context, db *sql.DB) error {
	return nil
}

func TestWithDB_MissingEnvVars(t *testing.T) {
	os.Clearenv()
	err := withDB("", mockFn)
	if err == nil || !strings.Contains(err.Error(), "missing required connection parameters") {
		t.Errorf("expected missing env var error, got: %v", err)
	}
}

func TestWithDB_UsesDatabaseArg(t *testing.T) {
	os.Setenv("MSSQL_SERVER", "localhost")
	os.Setenv("MSSQL_PORT", "1433")
	os.Setenv("MSSQL_USER", "sa")
	os.Setenv("MSSQL_PASSWORD", "pass")
	os.Setenv("MSSQL_DATABASE", "shouldnotuse")
	dbName := "testdb"
	called := false
	fakeFn := func(ctx context.Context, db *sql.DB) error {
		called = true
		return nil
	}
	// Use sqlmock for safe DB mocking
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer mockDB.Close()

	origOpen := sqlOpen
	defer func() { sqlOpen = origOpen }()
	sqlOpen = func(driver, dsn string) (*sql.DB, error) {
		if !strings.Contains(dsn, dbName) {
			t.Errorf("expected dsn to contain %s, got: %s", dbName, dsn)
		}
		return mockDB, nil
	}
	origPing := dbPing
	defer func() { dbPing = origPing }()
	dbPing = func(db *sql.DB) error { return nil }

	err = withDB(dbName, fakeFn)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if !called {
		t.Error("expected callback to be called")
	}
}

// Use the real withDB and patch sqlOpen/dbPing via package-level vars for testability.
