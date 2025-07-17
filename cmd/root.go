package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	FlagServer   string
	FlagPort     string
	FlagUser     string
	FlagPassword string
	FlagDatabase string
)

var rootCmd = &cobra.Command{
	Use:   "getmssql",
	Short: "Export MSSQL data to various formats",
	Long:  `A CLI tool to export MSSQL data to JSON, CSV, TSV, SQLite, or DuckDB formats.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&FlagServer, "server", "", "MSSQL server hostname or IP (env: MSSQL_SERVER)")
	rootCmd.PersistentFlags().StringVar(&FlagPort, "port", "", "MSSQL server port (env: MSSQL_PORT)")
	rootCmd.PersistentFlags().StringVar(&FlagUser, "user", "", "MSSQL username (env: MSSQL_USER)")
	rootCmd.PersistentFlags().StringVar(&FlagPassword, "password", "", "MSSQL password (env: MSSQL_PASSWORD)")
	rootCmd.PersistentFlags().StringVar(&FlagDatabase, "database", "", "MSSQL database name (env: MSSQL_DATABASE)")
}
