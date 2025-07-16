package cli

import (
	"log"
	"strings"
)

// isInvalidTableError checks recursively for substrings indicating a missing/invalid table in any wrapped error.
// isInvalidTableError checks recursively for substrings indicating a missing/invalid table in any wrapped error.
// It matches common English and Spanish SQL Server and driver error messages.
func isInvalidTableError(err error) bool {
	// Always print the incoming error for debugging
	log.Printf("[DEBUG] isInvalidTableError called with: %v\n", err)
	// List of substrings that indicate a missing/invalid table error
	var patterns = []string{
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
	matched := false
	origErr := err
	for err != nil {
		errStr := strings.ToLower(err.Error())
		for _, pat := range patterns {
			if strings.Contains(errStr, pat) {
				matched = true
				return true
			}
		}
		// Special case: Spanish SQL Server error
		if strings.Contains(errStr, "no es válido") && strings.Contains(errStr, "nombre de objeto") {
			matched = true
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	if !matched && origErr != nil {
		// Debug print: log the original error string if no pattern matched
		// Remove or comment out this line after debugging
		log.Printf("[DEBUG] isInvalidTableError: no match for error: %v\n", origErr)
	}
	return false
}
