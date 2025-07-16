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
	server = strings.TrimSpace(os.Getenv("MSSQL_SERVER"))
	port = strings.TrimSpace(os.Getenv("MSSQL_PORT"))
	user = strings.TrimSpace(os.Getenv("MSSQL_USER"))
	password = strings.TrimSpace(os.Getenv("MSSQL_PASSWORD"))
	database = strings.TrimSpace(os.Getenv("MSSQL_DATABASE"))
	missingVars := []string{}
	if server == "" {
		missingVars = append(missingVars, "MSSQL_SERVER")
	}
	if port == "" {
		missingVars = append(missingVars, "MSSQL_PORT")
	}
	if user == "" {
		missingVars = append(missingVars, "MSSQL_USER")
	}
	if password == "" {
		missingVars = append(missingVars, "MSSQL_PASSWORD")
	}
	if database == "" {
		missingVars = append(missingVars, "MSSQL_DATABASE")
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
		err = fmt.Errorf("missing required environment variables: %s\n%s", strings.Join(missingVars, ", "), example)
	}
	return
}
