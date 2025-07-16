package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"getmssql/dbexport"

	"github.com/spf13/cobra"
)

var (
	downloadFields   string
	downloadFormat   string
	downloadDatabase string
)

var downloadCmd = &cobra.Command{
	Use:   "download <table>",
	Short: "Export data from a table",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		table := args[0]
		asTSV := downloadFormat == "tsv"
		asCSV := downloadFormat == "csv"
		asSQLite := downloadFormat == "sqlite3"
		asDuckDB := downloadFormat == "duckdb"
		return withDB(downloadDatabase, func(ctx context.Context, db *sql.DB) error {
			err := dbexport.DownloadTable(db, table, downloadFields, asTSV, asCSV, asSQLite, asDuckDB)
			if err != nil {
				if isInvalidTableError(err) {
					return fmt.Errorf("%v.\n\nverifica que el nombre de la tabla o vista exista en la base de datos y esté correctamente escrito. si pertenece a otro esquema, usa el nombre completo (por ejemplo: esquema.tabla)", err)
				}
			}
			return err
		})
	},
}

func init() {
	downloadCmd.Flags().StringVar(&downloadFields, "fields", "", "Comma-separated list of fields to export (optional)")
	downloadCmd.Flags().StringVar(&downloadFormat, "format", "json", "Export format: json, tsv, csv, sqlite3, duckdb")
	downloadCmd.Flags().StringVar(&downloadDatabase, "database", "", "MSSQL database name (env: MSSQL_DATABASE)")
	rootCmd.AddCommand(downloadCmd)
}
