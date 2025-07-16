package dbexport

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// ScanRowValues scans a row into a slice of values, converting types as needed.
func ScanRowValues(rows *sql.Rows, cols []string) []interface{} {
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	if err := rows.Scan(columnPointers...); err != nil {
		panic(fmt.Sprintf("Error scanning row: %v", err))
	}
	vals := make([]interface{}, len(cols))
	for i := range cols {
		val := columnPointers[i].(*interface{})
		v := *val
		switch t := v.(type) {
		case time.Time:
			vals[i] = t.Format("2006-01-02")
		case []uint8:
			s := string(t)
			if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
				vals[i] = intVal
			} else if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
				vals[i] = floatVal
			} else {
				vals[i] = s
			}
		default:
			vals[i] = v
		}
	}
	return vals
}

// ScanRowMap scans a row into a map of column names to values, converting types as needed.
func ScanRowMap(rows *sql.Rows, cols []string) map[string]interface{} {
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	if err := rows.Scan(columnPointers...); err != nil {
		panic(fmt.Sprintf("Error scanning row: %v", err))
	}
	rowMap := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		v := *val
		switch t := v.(type) {
		case time.Time:
			rowMap[colName] = t.Format("2006-01-02")
		case []uint8:
			s := string(t)
			if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
				rowMap[colName] = intVal
			} else if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
				rowMap[colName] = floatVal
			} else {
				rowMap[colName] = s
			}
		default:
			rowMap[colName] = v
		}
	}
	return rowMap
}
