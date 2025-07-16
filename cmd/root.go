package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
