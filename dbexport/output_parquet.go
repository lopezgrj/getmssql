package dbexport

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// ReadFieldsFile reads a file with one column name per line and returns a slice of column names.
func ReadFieldsFile(fieldsFile string) ([]string, error) {
	data, err := os.ReadFile(fieldsFile)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var columns []string
	for _, line := range lines {
		col := strings.TrimSpace(line)
		if col != "" {
			columns = append(columns, col)
		}
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found in fields file")
	}
	return columns, nil
}

// DownloadTableParquet exports a table to Parquet format using the provided DB and fields file.
func DownloadTableParquet(db *sql.DB, table string, fieldsFile string) error {
	columns, err := ReadFieldsFile(fieldsFile)
	if err != nil {
		return fmt.Errorf("failed to read fields file: %w", err)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ","), table)
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	// Output file name: table.parquet
	outFile := fmt.Sprintf("%s.parquet", strings.ToLower(table))
	outFile = filepath.Join(".", outFile)

	if _, err := os.Stat(outFile); err == nil {
		fmt.Printf("Output file %s already exists. Overwrite? [y/N]: ", outFile)
		var response string
		_, scanErr := fmt.Scanln(&response)
		if scanErr != nil || (strings.ToLower(strings.TrimSpace(response)) != "y" && strings.ToLower(strings.TrimSpace(response)) != "yes") {
			return fmt.Errorf("aborted by user; file exists: %s", outFile)
		}
		if rmErr := os.Remove(outFile); rmErr != nil {
			return fmt.Errorf("failed to remove existing file: %w", rmErr)
		}
	}

	// Get total row count for progress
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var totalRows int64
	err = db.QueryRowContext(context.Background(), countQuery).Scan(&totalRows)
	if err != nil {
		fmt.Printf("Warning: could not get total row count: %v\n", err)
	} else {
		fmt.Printf("Total rows to download: %d\n", totalRows)
	}

	fmt.Printf("Exporting table '%s' to Parquet file: %s...\n", table, outFile)
	start := time.Now()
	err = WriteParquet(rows, columns, outFile, start)
	duration := time.Since(start)
	if err != nil {
		return err
	}
	fmt.Printf("Export to Parquet complete: %s\n", outFile)
	fmt.Printf("Duration: %s\n", duration)
	return nil
}

// WriteParquet exports rows to a Parquet file.
func WriteParquet(rows *sql.Rows, columns []string, filePath string, now time.Time) error {
	// Handle Ctrl-C (SIGINT) for user abort
	abort := make(chan os.Signal, 1)
	signal.Notify(abort, os.Interrupt)
	defer signal.Stop(abort)
	if rows == nil {
		return fmt.Errorf("rows is nil")
	}

	// Open output file
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer fw.Close()

	// Dynamically build a struct type for columns
	typeFields := make([]reflect.StructField, len(columns))
	for i, col := range columns {
		typeFields[i] = reflect.StructField{
			Name: fmt.Sprintf("Col%d", i),
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(fmt.Sprintf("parquet:\"name=%s, type=BYTE_ARRAY, convertedtype=UTF8\"", strings.ToLower(col))),
		}
	}
	rowType := reflect.StructOf(typeFields)

	pw, err := writer.NewParquetWriter(fw, reflect.New(rowType).Interface(), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet struct writer: %w", err)
	}
	defer func() {
		if err := pw.WriteStop(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing parquet writer: %v\n", err)
		}
	}()

	vals := make([]interface{}, len(columns))
	valPtrs := make([]interface{}, len(columns))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	rowCount := 0
	aborted := false
	for rows.Next() {
		select {
		case <-abort:
			fmt.Fprintf(os.Stderr, "\nExport aborted by user (Ctrl-C).\n")
			aborted = true
		default:
		}
		if aborted {
			break
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return fmt.Errorf("scan error: %w", err)
		}
		rowVal := reflect.New(rowType).Elem()
		for i := range columns {
			v := vals[i]
			var s string
			if v == nil {
				s = ""
			} else {
				s = fmt.Sprintf("%v", v)
			}
			rowVal.Field(i).SetString(s)
		}
		if err := pw.Write(rowVal.Interface()); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("\r  ...%d rows written", rowCount)
		}
	}
	if aborted {
		return fmt.Errorf("export aborted by user")
	}
	fmt.Printf("\rTotal rows written to Parquet: %d\n", rowCount)
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}
	return nil
}
