//go:build integration
// +build integration

package cmd

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
)

// Integration test: requires a running MSSQL instance and env vars set.
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
