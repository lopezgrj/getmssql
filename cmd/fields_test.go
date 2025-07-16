package cmd

import (
	"bytes"
	"testing"
)

func TestFields_Help(t *testing.T) {
	   buf := new(bytes.Buffer)
	   fieldsCmd.SetOut(buf)
	   fieldsCmd.SetArgs([]string{"--help"})
	   err := fieldsCmd.Execute()
	   // Reset args after test to avoid state leakage
	   fieldsCmd.SetArgs([]string{})
	   if err != nil {
			   t.Fatalf("unexpected error: %v", err)
	   }
	   out := buf.String()
	   if !containsAll(out, []string{"Usage:", "fields <table>"}) {
			   t.Errorf("expected help output for fields, got: %s", out)
	   }
}
