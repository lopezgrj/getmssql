package dbexport

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// WriteFileOutput writes table data to a file in CSV, TSV, or JSON format.
func WriteFileOutput(rows *sql.Rows, cols []string, table string, asTSV, asCSV bool, start time.Time) error {
	var filename string
	if asCSV {
		filename = fmt.Sprintf("%s.csv", strings.ToLower(table))
	} else if asTSV {
		filename = fmt.Sprintf("%s.tsv", strings.ToLower(table))
	} else {
		filename = fmt.Sprintf("%s.json", strings.ToLower(table))
	}
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer file.Close()

	rowCount := 0
	if asCSV {
		file.WriteString(strings.Join(cols, "||") + "\n")
	} else if asTSV {
		file.WriteString(strings.Join(cols, "\t") + "\n")
	} else {
		file.WriteString("[")
	}
	first := true
	for rows.Next() {
		rowMap := ScanRowMap(rows, cols)
		if asCSV {
			var rowVals []string
			for _, colName := range cols {
				val := rowMap[colName]
				switch v := val.(type) {
				case nil:
					rowVals = append(rowVals, "")
				case string:
					rowVals = append(rowVals, v)
				case []byte:
					rowVals = append(rowVals, string(v))
				default:
					rowVals = append(rowVals, fmt.Sprintf("%v", v))
				}
			}
			file.WriteString(strings.Join(rowVals, "||") + "\n")
		} else if asTSV {
			var rowVals []string
			for _, colName := range cols {
				val := rowMap[colName]
				switch v := val.(type) {
				case nil:
					rowVals = append(rowVals, "")
				case string:
					rowVals = append(rowVals, v)
				case []byte:
					rowVals = append(rowVals, string(v))
				default:
					rowVals = append(rowVals, fmt.Sprintf("%v", v))
				}
			}
			file.WriteString(strings.Join(rowVals, "\t") + "\n")
		} else {
			if !first {
				file.WriteString(",")
			}
			first = false
			jsonBytes, _ := json.Marshal(rowMap)
			file.Write(jsonBytes)
		}
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("\rDownloaded %d rows...", rowCount)
		}
	}
	if !asCSV && !asTSV {
		file.WriteString("]")
	}
	fmt.Printf("\rTotal rows downloaded: %d\n", rowCount)
	elapsed := time.Since(start)
	fmt.Printf("Table '%s' data written to %s in %s\n", table, filename, elapsed)
	return nil
}

// WriteFileOutputRows is a wrapper for WriteFileOutput that accepts Rows interface
func WriteFileOutputRows(rows Rows, cols []string, table string, asTSV, asCSV bool, start time.Time) error {
	sqlRows, ok := rows.(*sql.Rows)
	if !ok {
		return fmt.Errorf("WriteFileOutput requires *sql.Rows")
	}
	return WriteFileOutput(sqlRows, cols, table, asTSV, asCSV, start)
}
