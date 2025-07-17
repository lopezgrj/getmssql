package cmd

import (
	"errors"
	"fmt"
	"testing"
)

type wrapErr struct{ inner error }

func (w wrapErr) Error() string { return "wrap: " + w.inner.Error() }
func (w wrapErr) Unwrap() error { return w.inner }

func TestIsInvalidTableError_CombinedSpanishOnly(t *testing.T) {
	err := fmt.Errorf("nombre de objeto foo no es válido")
	if !isInvalidTableError(err) {
		t.Error("expected true for combined Spanish substring match")
	}
}

func TestIsInvalidTableError_UnwrapNoMatch(t *testing.T) {
	err := wrapErr{customErr{}}
	if isInvalidTableError(err) {
		t.Error("expected false for wrapped custom error")
	}
}

func TestIsInvalidTableError_Nil(t *testing.T) {
	if isInvalidTableError(nil) {
		t.Error("expected false for nil error")
	}
}

type customErr struct{}

func (c customErr) Error() string { return "definitely not a table error" }

func TestIsInvalidTableError_NoMatch_TriggersLog(t *testing.T) {
	err := customErr{}
	if isInvalidTableError(err) {
		t.Error("expected false for custom error")
	}
}

func TestIsInvalidTableError_MatchesPatterns(t *testing.T) {
	patterns := []string{
		"could not get total row count",
		"is not a valid object name",
		"invalid object name",
		"el nombre de objeto",
		"no es válido",
		"does not exist",
		"no existe",
		"object does not exist",
		"table does not exist",
		"invalid table name",
		"could not find object",
		"no such table",
	}
	for _, pat := range patterns {
		err := fmt.Errorf("some error: %s", pat)
		if !isInvalidTableError(err) {
			t.Errorf("expected true for pattern: %q", pat)
		}
	}
}

func TestIsInvalidTableError_CombinedSpanish(t *testing.T) {
	err := fmt.Errorf("el nombre de objeto foo no es válido")
	if !isInvalidTableError(err) {
		t.Error("expected true for combined Spanish error")
	}
}

func TestIsInvalidTableError_Unwrap(t *testing.T) {
	base := errors.New("invalid object name")
	wrapped := fmt.Errorf("wrap1: %w", base)
	if !isInvalidTableError(wrapped) {
		t.Error("expected true for wrapped error")
	}
}

func TestIsInvalidTableError_NoMatch(t *testing.T) {
	err := errors.New("some unrelated error")
	if isInvalidTableError(err) {
		t.Error("expected false for unrelated error")
	}
}
