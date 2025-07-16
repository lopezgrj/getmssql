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

// withDB handles DB connection, context, signal handling, and cleanup. It calls fn with the DB and context.
func withDB(database string, fn func(ctx context.Context, db *sql.DB) error) error {
	_ = godotenv.Load()
	server := os.Getenv("MSSQL_SERVER")
	port := os.Getenv("MSSQL_PORT")
	user := os.Getenv("MSSQL_USER")
	password := os.Getenv("MSSQL_PASSWORD")
	if database == "" {
		database = os.Getenv("MSSQL_DATABASE")
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
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return fmt.Errorf("error creating connection pool: %v", err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("cannot connect to database: %v", err)
	}
	fmt.Println("Connected to MSSQL successfully!")
	return fn(ctx, db)
}
