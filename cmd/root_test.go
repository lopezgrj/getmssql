package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestRoot_Help(t *testing.T) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetArgs([]string{"--help"})
	err := rootCmd.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Usage:") {
		t.Errorf("expected usage output, got: %s", out)
	}
}

func TestRoot_UnknownCommand(t *testing.T) {
	   buf := new(bytes.Buffer)
	   rootCmd.SetOut(buf)
	   rootCmd.SetArgs([]string{"notacommand"})
	   err := rootCmd.Execute()
	   if err == nil || !strings.Contains(err.Error(), "unknown command") {
			   t.Errorf("expected unknown command error, got: %v", err)
	   }
}
