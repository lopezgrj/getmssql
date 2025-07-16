package cmd

import (
	"bytes"
	"testing"
)

func TestFields_Help(t *testing.T) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetArgs([]string{"fields", "--help"})
	err := rootCmd.Execute()
	// Reset args after test to avoid state leakage
	rootCmd.SetArgs([]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !containsAll(out, []string{"Usage:", "fields <table>"}) {
		t.Errorf("expected help output for fields, got: %s", out)
	}
}
