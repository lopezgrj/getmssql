package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("getmssql version 1.0.0") // TODO: Use actual version variable
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
