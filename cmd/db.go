// Package cmd contains the command-line interface logic for the getmssql tool.
//
// This file provides helpers for establishing and managing database connections
// and context/signal handling for CLI commands that interact with SQL Server.
//
// Environment variables required for connection:
//   - MSSQL_SERVER:   SQL Server hostname or IP
//   - MSSQL_PORT:     SQL Server port
//   - MSSQL_USER:     SQL Server username
//   - MSSQL_PASSWORD: SQL Server password
//   - MSSQL_DATABASE: Database name (can be overridden per call)
//
// Optionally, a .env file can be used for local development.
package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/joho/godotenv"
)

// withDB establishes a connection to the SQL Server database, sets up context and signal handling,
// and ensures proper cleanup. It loads environment variables (optionally from a .env file),
// validates required parameters, and calls the provided function with a live DB connection and context.
//
// Usage:
//
//	err := withDB("", func(ctx context.Context, db *sql.DB) error {
//	    // use db here
//	    return nil
//	})
//
// If any required environment variable is missing or the connection fails, an error is returned.
// sqlOpen and dbPing are package-level variables to allow test injection.
var sqlOpen = sql.Open
var dbPing = func(db *sql.DB) error { return db.Ping() }

func withDB(database string, fn func(ctx context.Context, db *sql.DB) error) error {
	_ = godotenv.Load()
	// Prefer CLI flags, fallback to env vars
	server := FlagServer
	if server == "" {
		server = os.Getenv("MSSQL_SERVER")
	}
	port := FlagPort
	if port == "" {
		port = os.Getenv("MSSQL_PORT")
	}
	user := FlagUser
	if user == "" {
		user = os.Getenv("MSSQL_USER")
	}
	password := FlagPassword
	if password == "" {
		password = os.Getenv("MSSQL_PASSWORD")
	}
	if database == "" {
		database = FlagDatabase
		if database == "" {
			database = os.Getenv("MSSQL_DATABASE")
		}
	}
	missing := []string{}
	if server == "" {
		missing = append(missing, "MSSQL_SERVER")
	}
	if port == "" {
		missing = append(missing, "MSSQL_PORT")
	}
	if user == "" {
		missing = append(missing, "MSSQL_USER")
	}
	if password == "" {
		missing = append(missing, "MSSQL_PASSWORD")
	}
	if database == "" {
		missing = append(missing, "MSSQL_DATABASE")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required connection parameters: %s", strings.Join(missing, ", "))
	}
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=disable", server, user, password, port, database)
	db, err := sqlOpen("sqlserver", connString)
	if err != nil {
		return fmt.Errorf("error creating connection pool: %v", err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := dbPing(db); err != nil {
		return fmt.Errorf("cannot connect to database: %v", err)
	}
	fmt.Println("Connected to MSSQL successfully!")
	return fn(ctx, db)
}
