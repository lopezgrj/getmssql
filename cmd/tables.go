package cmd

import (
	"context"
	"database/sql"
	"getmssql/dbexport"

	"github.com/spf13/cobra"
)

var tablesCmd = &cobra.Command{
	Use:   "tables",
	Short: "List all tables in the database",
	RunE: func(cmd *cobra.Command, args []string) error {
		return withDB("", func(ctx context.Context, db *sql.DB) error {
			return dbexport.ListTables(db)
		})
	},
}

func init() {
	rootCmd.AddCommand(tablesCmd)
}
