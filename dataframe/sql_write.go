package dataframe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SQLWriteOption configures how a DataFrame is written to a SQL database
type SQLWriteOption struct {
	// IfExists specifies what to do if the table already exists
	// Options: "fail" (default), "replace" (DROP then CREATE), "append" (insert into existing)
	IfExists string

	// Dialect specifies the SQL dialect to use: "sqlite", "postgres", "mysql"
	// If empty, the dialect will be auto-detected from the database driver
	Dialect string

	// BatchSize specifies how many rows to insert per batch
	// Default: 1000
	BatchSize int

	// TypeMap allows custom SQL type mapping for specific columns
	// Map keys are column names, values are SQL type strings (e.g., "VARCHAR(255)", "INTEGER PRIMARY KEY")
	TypeMap map[string]string

	// CreateTable specifies whether to auto-create the table if it doesn't exist
	// Default: true
	CreateTable bool
}

// ToSQL writes the DataFrame to a SQL table with auto-commit
func (df *DataFrame) ToSQL(db *sql.DB, tableName string, options ...SQLWriteOption) error {
	return df.ToSQLContext(context.Background(), db, tableName, options...)
}

// ToSQLContext writes the DataFrame to a SQL table with auto-commit and context support
func (df *DataFrame) ToSQLContext(ctx context.Context, db *sql.DB, tableName string, options ...SQLWriteOption) error {
	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Use transaction-based implementation
	if err := df.ToSQLTxContext(ctx, tx, tableName, options...); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// ToSQLTx writes the DataFrame to a SQL table using an existing transaction
func (df *DataFrame) ToSQLTx(tx *sql.Tx, tableName string, options ...SQLWriteOption) error {
	return df.ToSQLTxContext(context.Background(), tx, tableName, options...)
}

// ToSQLTxContext writes the DataFrame to a SQL table using an existing transaction with context support
func (df *DataFrame) ToSQLTxContext(ctx context.Context, tx *sql.Tx, tableName string, options ...SQLWriteOption) error {
	// Validate user options first (before applying defaults)
	if len(options) > 0 {
		userOpt := options[0]

		// Validate IfExists if provided
		if userOpt.IfExists != "" {
			switch userOpt.IfExists {
			case "fail", "replace", "append":
				// Valid
			default:
				return fmt.Errorf("invalid IfExists option: %s (must be 'fail', 'replace', or 'append')", userOpt.IfExists)
			}
		}

		// Validate BatchSize if provided (non-zero means explicitly set)
		if userOpt.BatchSize != 0 && userOpt.BatchSize <= 0 {
			return fmt.Errorf("BatchSize must be greater than 0, got %d", userOpt.BatchSize)
		}

		// Validate Dialect if provided
		if userOpt.Dialect != "" {
			switch strings.ToLower(userOpt.Dialect) {
			case "sqlite", "sqlite3", "postgres", "postgresql", "pq", "mysql":
				// Valid
			default:
				return fmt.Errorf("unknown dialect: %s (supported: sqlite, postgres, mysql)", userOpt.Dialect)
			}
		}
	}

	// Parse options with defaults
	opts := SQLWriteOption{
		IfExists:    "fail",
		BatchSize:   1000,
		CreateTable: true,
	}

	if len(options) > 0 {
		userOpt := options[0]
		if userOpt.IfExists != "" {
			opts.IfExists = userOpt.IfExists
		}
		if userOpt.BatchSize > 0 {
			opts.BatchSize = userOpt.BatchSize
		}
		if userOpt.Dialect != "" {
			opts.Dialect = userOpt.Dialect
		}
		if userOpt.TypeMap != nil {
			opts.TypeMap = userOpt.TypeMap
		}
		// Note: We don't override CreateTable to preserve the default value of true
		// If users need to disable table creation, they should not use this function
	}

	// Get database from transaction
	// We need a *sql.DB for dialect detection, but we only have *sql.Tx
	// We'll use a workaround: try to detect from the driver type
	var dialect SQLDialect
	var err error

	if opts.Dialect != "" {
		// Use explicitly specified dialect
		switch strings.ToLower(opts.Dialect) {
		case "sqlite", "sqlite3":
			dialect = &SQLiteDialect{}
		case "postgres", "postgresql", "pq":
			dialect = &PostgresDialect{}
		case "mysql":
			dialect = &MySQLDialect{}
		default:
			return fmt.Errorf("unknown dialect: %s (supported: sqlite, postgres, mysql)", opts.Dialect)
		}
	} else {
		// Try to detect dialect from the transaction's driver
		// This is tricky since sql.Tx doesn't expose the driver directly
		// We'll default to SQLite and let the user specify if needed
		dialect = &SQLiteDialect{}
	}

	// Check if table exists
	exists, err := tableExistsTx(ctx, tx, tableName, dialect)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %w", err)
	}

	// Handle IfExists logic
	if exists {
		switch opts.IfExists {
		case "fail":
			return fmt.Errorf("table %s already exists", tableName)
		case "replace":
			// Drop the table
			dropSQL := fmt.Sprintf("DROP TABLE %s", dialect.QuoteIdentifier(tableName))
			if _, err := tx.ExecContext(ctx, dropSQL); err != nil {
				return fmt.Errorf("error dropping table: %w", err)
			}
			exists = false // Table no longer exists
		case "append":
			// Table exists, we'll append to it (no action needed here)
		}
	}

	// Create table if it doesn't exist and CreateTable is true
	if !exists && opts.CreateTable {
		if err := createTableTx(ctx, tx, tableName, df, dialect, opts.TypeMap); err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}
	}

	// If DataFrame is empty, we're done
	if df.Nrows() == 0 {
		return nil
	}

	// Perform batch insert
	if err := batchInsertTx(ctx, tx, tableName, df, dialect, opts.BatchSize); err != nil {
		return fmt.Errorf("error inserting data: %w", err)
	}

	return nil
}

// tableExistsTx checks if a table exists in the database
func tableExistsTx(ctx context.Context, tx *sql.Tx, tableName string, dialect SQLDialect) (bool, error) {
	query := dialect.TableExistsSQL(tableName)
	var name string
	err := tx.QueryRowContext(ctx, query, tableName).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// createTableTx creates a new table with the appropriate schema
func createTableTx(ctx context.Context, tx *sql.Tx, tableName string, df *DataFrame, dialect SQLDialect, typeMap map[string]string) error {
	// Build column type map
	columns := make(map[string]string)

	for _, colName := range df.ColumnNames() {
		col, err := df.Select(colName)
		if err != nil {
			return fmt.Errorf("error selecting column %s: %w", colName, err)
		}

		// Check if user provided a custom type for this column
		if typeMap != nil {
			if customType, ok := typeMap[colName]; ok {
				columns[colName] = customType
				continue
			}
		}

		// Infer type from column data
		goType := inferGoTypeFromColumn(col)
		sqlType := dialect.GoTypeToSQLType(goType)
		columns[colName] = sqlType
	}

	// Generate CREATE TABLE SQL
	createSQL := dialect.CreateTableSQL(tableName, columns)

	// Execute CREATE TABLE
	if _, err := tx.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("error executing CREATE TABLE: %w", err)
	}

	return nil
}

// batchInsertTx performs batch insertion of rows
func batchInsertTx(ctx context.Context, tx *sql.Tx, tableName string, df *DataFrame, dialect SQLDialect, batchSize int) error {
	colNames := df.ColumnNames()
	nRows := df.Nrows()
	nCols := len(colNames)

	if nCols == 0 {
		return fmt.Errorf("cannot insert: DataFrame has no columns")
	}

	// Get all columns upfront
	columns := make([]*Column[any], nCols)
	for i, colName := range colNames {
		col, err := df.Select(colName)
		if err != nil {
			return fmt.Errorf("error selecting column %s: %w", colName, err)
		}
		columns[i] = col
	}

	// Process in batches
	for batchStart := 0; batchStart < nRows; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > nRows {
			batchEnd = nRows
		}

		if err := insertBatch(ctx, tx, tableName, colNames, columns, batchStart, batchEnd, dialect); err != nil {
			return fmt.Errorf("error inserting batch (rows %d-%d): %w", batchStart, batchEnd-1, err)
		}
	}

	return nil
}

// insertBatch inserts a single batch of rows
func insertBatch(ctx context.Context, tx *sql.Tx, tableName string, colNames []string, columns []*Column[any], startIdx, endIdx int, dialect SQLDialect) error {
	nRows := endIdx - startIdx
	nCols := len(colNames)

	// Build quoted column names
	quotedCols := make([]string, nCols)
	for i, colName := range colNames {
		quotedCols[i] = dialect.QuoteIdentifier(colName)
	}

	// Build placeholders for multi-row INSERT
	// Example: INSERT INTO table (col1, col2) VALUES (?, ?), (?, ?), (?, ?)
	var placeholderRows []string
	placeholderIdx := 1
	for i := 0; i < nRows; i++ {
		var rowPlaceholders []string
		for j := 0; j < nCols; j++ {
			rowPlaceholders = append(rowPlaceholders, dialect.Placeholder(placeholderIdx))
			placeholderIdx++
		}
		placeholderRows = append(placeholderRows, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	// Build INSERT statement
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		dialect.QuoteIdentifier(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholderRows, ", "),
	)

	// Build args array
	args := make([]any, 0, nRows*nCols)
	for rowIdx := startIdx; rowIdx < endIdx; rowIdx++ {
		for colIdx := 0; colIdx < nCols; colIdx++ {
			value := columns[colIdx].Data[rowIdx]
			// Wrap in sql.Null* type to handle nil values properly
			args = append(args, convertGoTypeToSQLNullable(value))
		}
	}

	// Execute INSERT
	if _, err := tx.ExecContext(ctx, insertSQL, args...); err != nil {
		return err
	}

	return nil
}
