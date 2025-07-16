package cmd

import "strings"

// containsAll returns true if all substrings in subs are present in s.
func containsAll(s string, subs []string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
