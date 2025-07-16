package cmd

import (
	"bytes"
	"testing"
)

func TestTables_Help(t *testing.T) {
	buf := new(bytes.Buffer)
	tablesCmd.SetOut(buf)
	tablesCmd.SetArgs([]string{"--help"})
	err := tablesCmd.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !containsAll(out, []string{"Usage:", "tables"}) {
		t.Errorf("expected help output for tables, got: %s", out)
	}
}
