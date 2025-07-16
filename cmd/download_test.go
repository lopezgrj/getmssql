package cmd

import (
	"bytes"
	"testing"
)

func TestDownload_Help(t *testing.T) {
	buf := new(bytes.Buffer)
	downloadCmd.SetOut(buf)
	downloadCmd.SetArgs([]string{"--help"})
	err := downloadCmd.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !containsAll(out, []string{"Usage:", "download <table>"}) {
		t.Errorf("expected help output for download, got: %s", out)
	}
}
