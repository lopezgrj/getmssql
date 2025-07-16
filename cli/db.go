package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// withDB handles DB connection, context, signal handling, and cleanup. It calls fn with the DB and context.
func withDB(fn func(ctx context.Context, db *sql.DB) error) error {
	server, port, user, password, database, err := loadAndValidateEnv()
	if err != nil {
		return err
	}
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=disable", server, user, password, port, database)
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return fmt.Errorf("error creating connection pool: %v", err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)
	var signalErr error
	go func() {
		<-ctx.Done()
		select {
		case sig := <-interrupted:
			signalErr = fmt.Errorf("received signal: %v", sig)
			db.Close()
		default:
		}
	}()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("cannot connect to database: %v", err)
	}
	fmt.Println("Connected to MSSQL successfully!")
	if err := fn(ctx, db); err != nil {
		return err
	}
	if signalErr != nil {
		return signalErr
	}
	return nil
}
