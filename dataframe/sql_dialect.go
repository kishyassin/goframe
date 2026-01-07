package dataframe

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// SQLDialect defines the interface for database-specific SQL generation
type SQLDialect interface {
	// GoTypeToSQLType converts a Go type to the appropriate SQL type for this dialect
	GoTypeToSQLType(goType reflect.Type) string

	// Placeholder returns the placeholder syntax for a parameter at the given index
	// For example, PostgreSQL uses $1, $2, etc., while MySQL and SQLite use ?
	Placeholder(index int) string

	// QuoteIdentifier quotes a table or column name appropriately for this dialect
	// For example, PostgreSQL and SQLite use double quotes, MySQL uses backticks
	QuoteIdentifier(name string) string

	// CreateTableSQL generates a CREATE TABLE statement for this dialect
	CreateTableSQL(tableName string, columns map[string]string) string

	// TableExistsSQL returns a query to check if a table exists
	TableExistsSQL(tableName string) string
}

// SQLiteDialect implements SQLDialect for SQLite databases
type SQLiteDialect struct{}

// GoTypeToSQLType converts Go types to SQLite types
func (d *SQLiteDialect) GoTypeToSQLType(goType reflect.Type) string {
	// Handle pointer types
	if goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	switch goType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.String:
		return "TEXT"
	case reflect.Bool:
		return "INTEGER" // SQLite stores bool as 0/1
	default:
		// Check for time.Time
		if goType.String() == "time.Time" {
			return "TIMESTAMP"
		}
		// Default to TEXT for unknown types
		return "TEXT"
	}
}

// Placeholder returns the placeholder syntax for SQLite (always ?)
func (d *SQLiteDialect) Placeholder(index int) string {
	return "?"
}

// QuoteIdentifier quotes identifiers with double quotes
func (d *SQLiteDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}

// CreateTableSQL generates a CREATE TABLE statement for SQLite
func (d *SQLiteDialect) CreateTableSQL(tableName string, columns map[string]string) string {
	var columnDefs []string
	for colName, colType := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", d.QuoteIdentifier(colName), colType))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", d.QuoteIdentifier(tableName), strings.Join(columnDefs, ", "))
}

// TableExistsSQL returns a query to check if a table exists in SQLite
func (d *SQLiteDialect) TableExistsSQL(tableName string) string {
	return fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name=%s", d.Placeholder(1))
}

// PostgresDialect implements SQLDialect for PostgreSQL databases
type PostgresDialect struct{}

// GoTypeToSQLType converts Go types to PostgreSQL types
func (d *PostgresDialect) GoTypeToSQLType(goType reflect.Type) string {
	// Handle pointer types
	if goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	switch goType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
		return "BIGINT"
	case reflect.Uint8, reflect.Uint16:
		return "INTEGER"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.String:
		return "TEXT"
	case reflect.Bool:
		return "BOOLEAN"
	default:
		// Check for time.Time
		if goType.String() == "time.Time" {
			return "TIMESTAMP"
		}
		// Default to TEXT for unknown types
		return "TEXT"
	}
}

// Placeholder returns the placeholder syntax for PostgreSQL ($1, $2, etc.)
func (d *PostgresDialect) Placeholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

// QuoteIdentifier quotes identifiers with double quotes
func (d *PostgresDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}

// CreateTableSQL generates a CREATE TABLE statement for PostgreSQL
func (d *PostgresDialect) CreateTableSQL(tableName string, columns map[string]string) string {
	var columnDefs []string
	for colName, colType := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", d.QuoteIdentifier(colName), colType))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", d.QuoteIdentifier(tableName), strings.Join(columnDefs, ", "))
}

// TableExistsSQL returns a query to check if a table exists in PostgreSQL
func (d *PostgresDialect) TableExistsSQL(tableName string) string {
	return fmt.Sprintf("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename=%s", d.Placeholder(1))
}

// MySQLDialect implements SQLDialect for MySQL databases
type MySQLDialect struct{}

// GoTypeToSQLType converts Go types to MySQL types
func (d *MySQLDialect) GoTypeToSQLType(goType reflect.Type) string {
	// Handle pointer types
	if goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	switch goType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint32, reflect.Uint64:
		return "BIGINT"
	case reflect.Uint8, reflect.Uint16:
		return "INT"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.String:
		return "TEXT"
	case reflect.Bool:
		return "TINYINT(1)"
	default:
		// Check for time.Time
		if goType.String() == "time.Time" {
			return "DATETIME"
		}
		// Default to TEXT for unknown types
		return "TEXT"
	}
}

// Placeholder returns the placeholder syntax for MySQL (always ?)
func (d *MySQLDialect) Placeholder(index int) string {
	return "?"
}

// QuoteIdentifier quotes identifiers with backticks
func (d *MySQLDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", name)
}

// CreateTableSQL generates a CREATE TABLE statement for MySQL
func (d *MySQLDialect) CreateTableSQL(tableName string, columns map[string]string) string {
	var columnDefs []string
	for colName, colType := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", d.QuoteIdentifier(colName), colType))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", d.QuoteIdentifier(tableName), strings.Join(columnDefs, ", "))
}

// TableExistsSQL returns a query to check if a table exists in MySQL
func (d *MySQLDialect) TableExistsSQL(tableName string) string {
	return fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=%s", d.Placeholder(1))
}

// detectDialect attempts to detect the database dialect from the driver name
func detectDialect(db *sql.DB) (SQLDialect, error) {
	// Get the driver name using reflection
	// This is a bit hacky but works for standard sql.DB
	driver := fmt.Sprintf("%T", db.Driver())

	// Match common driver patterns
	driverLower := strings.ToLower(driver)
	if strings.Contains(driverLower, "sqlite") {
		return &SQLiteDialect{}, nil
	}
	if strings.Contains(driverLower, "postgres") || strings.Contains(driverLower, "pq") {
		return &PostgresDialect{}, nil
	}
	if strings.Contains(driverLower, "mysql") {
		return &MySQLDialect{}, nil
	}

	// Default to SQLite if we can't detect
	return &SQLiteDialect{}, fmt.Errorf("could not detect database dialect from driver %s, defaulting to SQLite", driver)
}

// getDialect returns the appropriate dialect based on the provided name or detects it
func getDialect(dialectName string, db *sql.DB) (SQLDialect, error) {
	// If dialect is explicitly specified, use it
	if dialectName != "" {
		switch strings.ToLower(dialectName) {
		case "sqlite", "sqlite3":
			return &SQLiteDialect{}, nil
		case "postgres", "postgresql", "pq":
			return &PostgresDialect{}, nil
		case "mysql":
			return &MySQLDialect{}, nil
		default:
			return nil, fmt.Errorf("unknown dialect: %s (supported: sqlite, postgres, mysql)", dialectName)
		}
	}

	// Otherwise, try to detect it
	return detectDialect(db)
}

// inferGoTypeFromValue infers the Go type from a value, handling nil appropriately
func inferGoTypeFromValue(value any) reflect.Type {
	if value == nil {
		// For nil values, we can't determine the type, so default to string
		return reflect.TypeOf("")
	}
	return reflect.TypeOf(value)
}

// inferGoTypeFromColumn infers the Go type from a column by examining its values
func inferGoTypeFromColumn(col *Column[any]) reflect.Type {
	// Try to find a non-nil value to infer the type
	for _, value := range col.Data {
		if value != nil {
			return reflect.TypeOf(value)
		}
	}

	// If all values are nil, default to string type
	return reflect.TypeOf("")
}

// convertGoTypeToSQLNullable wraps a value in the appropriate sql.Null* type for insertion
func convertGoTypeToSQLNullable(value any) any {
	if value == nil {
		// For nil values, we need to return a sql.Null* type with Valid=false
		// We'll default to sql.NullString since we can't determine the type
		return sql.NullString{Valid: false}
	}

	switch v := value.(type) {
	case string:
		return sql.NullString{String: v, Valid: true}
	case int, int8, int16, int32, int64:
		// Convert all int types to int64
		val := reflect.ValueOf(v).Convert(reflect.TypeOf(int64(0))).Int()
		return sql.NullInt64{Int64: val, Valid: true}
	case uint, uint8, uint16, uint32, uint64:
		// Convert all uint types to int64 (may lose precision for very large values)
		val := int64(reflect.ValueOf(v).Convert(reflect.TypeOf(uint64(0))).Uint())
		return sql.NullInt64{Int64: val, Valid: true}
	case float32, float64:
		// Convert all float types to float64
		val := reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()
		return sql.NullFloat64{Float64: val, Valid: true}
	case bool:
		return sql.NullBool{Bool: v, Valid: true}
	case time.Time:
		return sql.NullTime{Time: v, Valid: true}
	default:
		// For unknown types, convert to string
		return sql.NullString{String: fmt.Sprintf("%v", v), Valid: true}
	}
}
