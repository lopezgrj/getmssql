package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"getmssql/dbexport"

	"github.com/spf13/cobra"
)

var fieldsCmd = &cobra.Command{
	Use:   "fields <table>",
	Short: "List all fields in the specified table",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		table := args[0]
		return withDB("", func(ctx context.Context, db *sql.DB) error {
			err := dbexport.ListFields(db, table)
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
	rootCmd.AddCommand(fieldsCmd)
}
