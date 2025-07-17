//go:build integration
// +build integration

// Integration tests for the getmssql CLI tool.
//
// These tests require a running Microsoft SQL Server instance and the environment variable MSSQL_TEST_DSN to be set
// to a valid connection string. To run these tests, use:
//
//     go test -tags=integration ./cmd
//
// The tests will be skipped if the required environment variable is not set.

package cmd

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
)

// TestIntegration_DBConnection verifies that a connection can be established to a running SQL Server instance.
// It requires the MSSQL_TEST_DSN environment variable to be set to a valid DSN.
// The test is skipped if the variable is not set.
func TestIntegration_DBConnection(t *testing.T) {
	dsn := os.Getenv("MSSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MSSQL_TEST_DSN not set; skipping integration test")
	}
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("failed to ping DB: %v", err)
	}
}
