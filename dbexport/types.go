package dbexport

// Rows is a minimal interface for *sql.Rows and test wrappers
// Used for dependency injection and testability in output writers.
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Columns() ([]string, error)
	Close() error
	Err() error
}
