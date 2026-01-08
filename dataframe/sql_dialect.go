package dataframe

import (
	"fmt"
	"reflect"
	"strings"
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
