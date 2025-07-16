package dbexport

import (
	"database/sql"
	"fmt"
)

func ListTables(db *sql.DB) error {
	query := `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME`
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying tables: %w", err)
	}
	defer rows.Close()

	fmt.Println("Tables in the database:")
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("error scanning table name: %w", err)
		}
		fmt.Println(tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error: %w", err)
	}
	return nil
}

func ListFields(db *sql.DB, table string) error {
	query := `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1 ORDER BY ORDINAL_POSITION`
	rows, err := db.Query(query, table)
	if err != nil {
		return fmt.Errorf("error querying fields: %w", err)
	}
	defer rows.Close()

	fmt.Printf("Fields in table '%s':\n", table)
	fmt.Println("Column Name\tType\tNullable")
	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return fmt.Errorf("error scanning field: %w", err)
		}
		fmt.Printf("%s\t%s\t%s\n", colName, dataType, isNullable)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error: %w", err)
	}
	return nil
}
