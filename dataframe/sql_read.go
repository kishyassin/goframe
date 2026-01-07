package dataframe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SQLReadOption configures how data is read from a database
type SQLReadOption struct {
	// NullHandler specifies how to handle NULL values in the result set
	// Options:
	//   - "nil" (default): SQL NULL → Go nil
	//   - "zero": SQL NULL → zero value (0, "", false)
	//   - "skip_row": Skip entire row if any column is NULL
	//   - map[string]any: Custom default values per column
	NullHandler any

	// ParseDates lists column names to parse as time.Time (optional)
	ParseDates []string
}

// FromSQL reads a SQL query into a DataFrame with auto-commit
func FromSQL(db *sql.DB, query string, args []any, options ...SQLReadOption) (*DataFrame, error) {
	return FromSQLContext(context.Background(), db, query, args, options...)
}

// FromSQLContext reads a SQL query into a DataFrame with context support
func FromSQLContext(ctx context.Context, db *sql.DB, query string, args []any, options ...SQLReadOption) (*DataFrame, error) {
	// Execute query
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Parse rows into DataFrame
	return fromSQLRows(rows, options...)
}

// FromSQLTx reads from an existing transaction
func FromSQLTx(tx *sql.Tx, query string, args []any, options ...SQLReadOption) (*DataFrame, error) {
	return FromSQLTxContext(context.Background(), tx, query, args, options...)
}

// FromSQLTxContext reads from an existing transaction with context support
func FromSQLTxContext(ctx context.Context, tx *sql.Tx, query string, args []any, options ...SQLReadOption) (*DataFrame, error) {
	// Execute query in transaction
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Parse rows into DataFrame
	return fromSQLRows(rows, options...)
}

// fromSQLRows is the core implementation that converts sql.Rows to DataFrame
func fromSQLRows(rows *sql.Rows, options ...SQLReadOption) (*DataFrame, error) {
	// Parse options
	opts := SQLReadOption{
		NullHandler: "nil", // default
	}
	if len(options) > 0 {
		userOpt := options[0]
		if userOpt.NullHandler != nil {
			opts.NullHandler = userOpt.NullHandler
		}
		if userOpt.ParseDates != nil {
			opts.ParseDates = userOpt.ParseDates
		}
	}

	// Get column metadata
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error getting column types: %w", err)
	}

	columnNames := make([]string, len(columnTypes))
	for i, col := range columnTypes {
		columnNames[i] = col.Name()
	}

	// Create scan destinations for each column
	scanDest := make([]any, len(columnTypes))
	for i := range columnTypes {
		scanDest[i] = createScanDestination(columnTypes[i])
	}

	// Collect rows
	var rowData [][]any
	for rows.Next() {
		// Scan row
		if err := rows.Scan(scanDest...); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// Extract values and apply NULL handling
		rowValues := make([]any, len(columnNames))
		skipRow := false
		for i, colName := range columnNames {
			value, err := extractValue(scanDest[i], colName, opts.NullHandler)
			if err != nil {
				// Special case: skip_row
				if err.Error() == "skip_row" {
					skipRow = true
					break
				}
				return nil, err
			}
			rowValues[i] = value
		}

		if skipRow {
			continue
		}

		rowData = append(rowData, rowValues)
	}

	// Check for errors from iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Build DataFrame from collected data
	df := NewDataFrame()
	for i, colName := range columnNames {
		// Collect column data
		colData := make([]any, len(rowData))
		for j, row := range rowData {
			colData[j] = row[i]
		}

		// Create and add column
		col := NewColumn(colName, colData)
		df.AddColumn(col)
	}

	return df, nil
}

// createScanDestination creates the appropriate sql.Null* type for scanning
func createScanDestination(colType *sql.ColumnType) any {
	// Try to get the database type name
	dbType := strings.ToUpper(colType.DatabaseTypeName())

	// Map common SQL types to sql.Null* types
	switch {
	case strings.Contains(dbType, "INT"):
		return new(sql.NullInt64)
	case strings.Contains(dbType, "FLOAT") || strings.Contains(dbType, "REAL") ||
		strings.Contains(dbType, "DOUBLE") || strings.Contains(dbType, "NUMERIC"):
		return new(sql.NullFloat64)
	case strings.Contains(dbType, "BOOL"):
		return new(sql.NullBool)
	case strings.Contains(dbType, "TIME") || strings.Contains(dbType, "DATE"):
		return new(sql.NullTime)
	case strings.Contains(dbType, "TEXT") || strings.Contains(dbType, "CHAR") ||
		strings.Contains(dbType, "VARCHAR"):
		return new(sql.NullString)
	default:
		// Default to NullString for unknown types
		return new(sql.NullString)
	}
}

// extractValue extracts the value from a sql.Null* type and applies NULL handling
func extractValue(dest any, colName string, nullHandler any) (any, error) {
	var value any
	var isNull bool

	// Extract value from sql.Null* types
	switch v := dest.(type) {
	case *sql.NullString:
		if v.Valid {
			value = v.String
		} else {
			isNull = true
		}
	case *sql.NullInt64:
		if v.Valid {
			value = v.Int64
		} else {
			isNull = true
		}
	case *sql.NullFloat64:
		if v.Valid {
			value = v.Float64
		} else {
			isNull = true
		}
	case *sql.NullBool:
		if v.Valid {
			value = v.Bool
		} else {
			isNull = true
		}
	case *sql.NullTime:
		if v.Valid {
			value = v.Time
		} else {
			isNull = true
		}
	default:
		return nil, fmt.Errorf("unsupported scan destination type: %T", dest)
	}

	// If not null, return the value
	if !isNull {
		return value, nil
	}

	// Apply NULL handling strategy
	return handleNull(colName, nullHandler, dest)
}

// handleNull applies the NULL handling strategy
func handleNull(colName string, nullHandler any, dest any) (any, error) {
	switch h := nullHandler.(type) {
	case string:
		switch h {
		case "nil":
			return nil, nil
		case "zero":
			// Return zero value for the type
			switch dest.(type) {
			case *sql.NullString:
				return "", nil
			case *sql.NullInt64:
				return int64(0), nil
			case *sql.NullFloat64:
				return float64(0), nil
			case *sql.NullBool:
				return false, nil
			case *sql.NullTime:
				return nil, nil // time.Time zero value is not very useful
			default:
				return nil, nil
			}
		case "skip_row":
			// Signal to skip this row
			return nil, fmt.Errorf("skip_row")
		default:
			return nil, fmt.Errorf("unknown null handler: %s", h)
		}
	case map[string]any:
		// Check if there's a custom default for this column
		if defaultVal, exists := h[colName]; exists {
			return defaultVal, nil
		}
		// If no custom default, use nil
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid null handler type: %T", nullHandler)
	}
}
