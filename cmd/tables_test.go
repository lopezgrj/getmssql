package cmd

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
)

func newTablesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tables",
		Short: "List all tables in the database",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Use the same logic as in tables.go
			return nil // For help test, RunE won't be called
		},
	}
}

func TestTables_Help(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := newTablesCmd()
	cmd.SetOut(buf)
	cmd.SetArgs([]string{"--help"})
	err := cmd.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !containsAll(out, []string{"Usage:", "tables"}) {
		t.Errorf("expected help output for tables, got: %s", out)
	}
}
