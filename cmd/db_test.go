package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestWithDB_MissingEachEnvVar(t *testing.T) {
	vars := []string{"MSSQL_SERVER", "MSSQL_PORT", "MSSQL_USER", "MSSQL_PASSWORD", "MSSQL_DATABASE"}
	for _, v := range vars {
		os.Setenv("MSSQL_SERVER", "localhost")
		os.Setenv("MSSQL_PORT", "1433")
		os.Setenv("MSSQL_USER", "sa")
		os.Setenv("MSSQL_PASSWORD", "pass")
		os.Setenv("MSSQL_DATABASE", "testdb")
		os.Unsetenv(v)
		err := withDB("", mockFn)
		if err == nil || !strings.Contains(err.Error(), v) {
			t.Errorf("expected error mentioning %s, got: %v", v, err)
		}
	}
}

func TestWithDB_SuccessfulConnectionAndCallback(t *testing.T) {
	os.Setenv("MSSQL_SERVER", "localhost")
	os.Setenv("MSSQL_PORT", "1433")
	os.Setenv("MSSQL_USER", "sa")
	os.Setenv("MSSQL_PASSWORD", "pass")
	os.Setenv("MSSQL_DATABASE", "testdb")
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer mockDB.Close()
	origOpen := sqlOpen
	defer func() { sqlOpen = origOpen }()
	sqlOpen = func(driver, dsn string) (*sql.DB, error) {
		return mockDB, nil
	}
	origPing := dbPing
	defer func() { dbPing = origPing }()
	dbPing = func(db *sql.DB) error { return nil }
	called := false
	err = withDB("", func(ctx context.Context, db *sql.DB) error {
		called = true
		return nil
	})
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if !called {
		t.Error("expected callback to be called")
	}
}

// mockFn is a helper to test the callback logic.
func mockFn(ctx context.Context, db *sql.DB) error {
	return nil
}

func TestWithDB_ConnectionError(t *testing.T) {
	os.Setenv("MSSQL_SERVER", "localhost")
	os.Setenv("MSSQL_PORT", "1433")
	os.Setenv("MSSQL_USER", "sa")
	os.Setenv("MSSQL_PASSWORD", "pass")
	os.Setenv("MSSQL_DATABASE", "testdb")
	origOpen := sqlOpen
	defer func() { sqlOpen = origOpen }()
	sqlOpen = func(driver, dsn string) (*sql.DB, error) {
		return nil, fmt.Errorf("open fail")
	}
	err := withDB("", mockFn)
	if err == nil || !strings.Contains(err.Error(), "error creating connection pool") {
		t.Errorf("expected connection pool error, got: %v", err)
	}
}

func TestWithDB_PingError(t *testing.T) {
	os.Setenv("MSSQL_SERVER", "localhost")
	os.Setenv("MSSQL_PORT", "1433")
	os.Setenv("MSSQL_USER", "sa")
	os.Setenv("MSSQL_PASSWORD", "pass")
	os.Setenv("MSSQL_DATABASE", "testdb")
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer mockDB.Close()
	origOpen := sqlOpen
	defer func() { sqlOpen = origOpen }()
	sqlOpen = func(driver, dsn string) (*sql.DB, error) {
		return mockDB, nil
	}
	origPing := dbPing
	defer func() { dbPing = origPing }()
	dbPing = func(db *sql.DB) error { return fmt.Errorf("ping fail") }
	err = withDB("", mockFn)
	if err == nil || !strings.Contains(err.Error(), "cannot connect to database") {
		t.Errorf("expected ping error, got: %v", err)
	}
}

func TestWithDB_CallbackError(t *testing.T) {
	os.Setenv("MSSQL_SERVER", "localhost")
	os.Setenv("MSSQL_PORT", "1433")
	os.Setenv("MSSQL_USER", "sa")
	os.Setenv("MSSQL_PASSWORD", "pass")
	os.Setenv("MSSQL_DATABASE", "testdb")
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer mockDB.Close()
	origOpen := sqlOpen
	defer func() { sqlOpen = origOpen }()
	sqlOpen = func(driver, dsn string) (*sql.DB, error) {
		return mockDB, nil
	}
	origPing := dbPing
	defer func() { dbPing = origPing }()
	dbPing = func(db *sql.DB) error { return nil }
	cbErr := fmt.Errorf("callback fail")
	err = withDB("", func(ctx context.Context, db *sql.DB) error { return cbErr })
	if err == nil || !strings.Contains(err.Error(), "callback fail") {
		t.Errorf("expected callback error, got: %v", err)
	}
}
