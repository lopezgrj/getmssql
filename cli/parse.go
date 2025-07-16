package cli

import (
	"flag"
	"fmt"
)

// parseTables parses the 'tables' subcommand flags.
func parseTables(args []string) error {
	tablesCmd := flag.NewFlagSet("tables", flag.ExitOnError)
	if err := tablesCmd.Parse(args); err != nil {
		return err
	}
	return nil
}

// parseFields parses the 'fields' subcommand flags and validates arguments.
func parseFields(args []string) (string, error) {
	fieldsCmd := flag.NewFlagSet("fields", flag.ExitOnError)
	if err := fieldsCmd.Parse(args); err != nil {
		return "", err
	}
	if fieldsCmd.NArg() < 1 {
		return "", fmt.Errorf("please provide a table name")
	}
	return fieldsCmd.Arg(0), nil
}

// parseDownload parses the 'download' subcommand flags and validates arguments.
func parseDownload(args []string) (table, fieldsFile, format string, err error) {
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)
	downloadFields := downloadCmd.String("fields", "", "File with list of fields to export (one per line)")
	downloadFormat := downloadCmd.String("format", "json", "Output format: json, tsv, csv, sqlite3, duckdb")
	if err := downloadCmd.Parse(args); err != nil {
		return "", "", "", err
	}
	if downloadCmd.NArg() < 1 {
		return "", "", "", fmt.Errorf("please provide a table name")
	}
	return downloadCmd.Arg(0), *downloadFields, *downloadFormat, nil
}
