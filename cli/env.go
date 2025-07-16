package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// loadAndValidateEnv loads .env and required MSSQL environment variables, returning them or logging fatally if missing.
// ...existing code...
func loadAndValidateEnv() (server, port, user, password, database string, err error) {
	_ = godotenv.Load()
	get := func(flagVal, envVar string) string {
		if flagVal != "" {
			return flagVal
		}
		return strings.TrimSpace(os.Getenv(envVar))
	}
	server = get(flagServer, "MSSQL_SERVER")
	port = get(flagPort, "MSSQL_PORT")
	user = get(flagUser, "MSSQL_USER")
	password = get(flagPassword, "MSSQL_PASSWORD")
	database = get(flagDatabase, "MSSQL_DATABASE")
	missingVars := []string{}
	if server == "" {
		missingVars = append(missingVars, "MSSQL_SERVER (or --server)")
	}
	if port == "" {
		missingVars = append(missingVars, "MSSQL_PORT (or --port)")
	}
	if user == "" {
		missingVars = append(missingVars, "MSSQL_USER (or --user)")
	}
	if password == "" {
		missingVars = append(missingVars, "MSSQL_PASSWORD (or --password)")
	}
	if database == "" {
		missingVars = append(missingVars, "MSSQL_DATABASE (or --database)")
	}
	if len(missingVars) > 0 {
		example := `
Example .env file:
MSSQL_SERVER=localhost
MSSQL_PORT=1433
MSSQL_USER=youruser
MSSQL_PASSWORD=yourpassword
MSSQL_DATABASE=yourdatabase
`
		err = fmt.Errorf("missing required connection parameters: %s\nYou can set these via environment variables or CLI flags.\n%s", strings.Join(missingVars, ", "), example)
	}
	return
}
