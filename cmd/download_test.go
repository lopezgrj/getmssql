package cmd

import (
	"bytes"
	"testing"
)

func TestDownload_Help(t *testing.T) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetArgs([]string{"download", "--help"})
	err := rootCmd.Execute()
	// Reset args after test to avoid state leakage
	rootCmd.SetArgs([]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !containsAll(out, []string{"Usage:", "download <table>"}) {
		t.Errorf("expected help output for download, got: %s", out)
	}
}
