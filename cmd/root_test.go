
package cmd

import (
	   "bytes"
	   "strings"
	   "sync/atomic"
	   "testing"
)


// Patch exitFunc for testing
var exitCode int32
var origExitFunc = exitFunc

func fakeExit(code int) {
	   atomic.StoreInt32(&exitCode, int32(code))
}

func restoreExitFunc() {
	   exitFunc = origExitFunc
}


func TestExecute_Success(t *testing.T) {
	   // Should not call exitFunc
	   exitFunc = fakeExit
	   defer restoreExitFunc()
	   atomic.StoreInt32(&exitCode, 0)
	   rootCmd.SetArgs([]string{"--help"})
	   Execute()
	   if atomic.LoadInt32(&exitCode) != 0 {
			   t.Errorf("unexpected exitFunc call: %d", exitCode)
	   }
}


func TestExecute_Error(t *testing.T) {
	   // Should call exitFunc(1) on error
	   exitFunc = fakeExit
	   defer restoreExitFunc()
	   atomic.StoreInt32(&exitCode, 0)
	   rootCmd.SetArgs([]string{"notacommand"})
	   Execute()
	   if atomic.LoadInt32(&exitCode) != 1 {
			   t.Errorf("expected exitFunc(1), got: %d", exitCode)
	   }
	   // Reset args after test
	   rootCmd.SetArgs([]string{})
	   fieldsCmd.SetArgs([]string{})
}

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
	// Reset args after test to avoid state leakage
	rootCmd.SetArgs([]string{})
	fieldsCmd.SetArgs([]string{})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("expected unknown command error, got: %v", err)
	}
}
