package goframe_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/kishyassin/goframe"
	_ "github.com/mattn/go-sqlite3"
)

// setupTestDB creates an in-memory SQLite database for testing
func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	return db
}

// TestFromSQL_BasicRead tests basic SELECT query functionality
func TestFromSQL_BasicRead(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, 'Bob', 30),
		(3, 'Charlie', 35)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read data using FromSQL
	df, err := goframe.FromSQL(db, "SELECT * FROM users ORDER BY id", nil)
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify row count
	if df.Nrows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Nrows())
	}

	// Verify column names
	colNames := df.ColumnNames()
	expectedCols := []string{"id", "name", "age"}
	if len(colNames) != len(expectedCols) {
		t.Errorf("Expected %d columns, got %d", len(expectedCols), len(colNames))
	}

	// Verify data
	nameCol, err := df.Select("name")
	if err != nil {
		t.Fatalf("Failed to select name column: %v", err)
	}
	if nameCol.Data[0] != "Alice" {
		t.Errorf("Expected 'Alice', got %v", nameCol.Data[0])
	}

	ageCol, err := df.Select("age")
	if err != nil {
		t.Fatalf("Failed to select age column: %v", err)
	}
	if ageCol.Data[1] != int64(30) {
		t.Errorf("Expected 30, got %v", ageCol.Data[1])
	}
}

// TestFromSQL_NullHandlerNil tests default NULL handling (nil)
func TestFromSQL_NullHandlerNil(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create table with nullable columns
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with NULLs
	_, err = db.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, NULL, 30),
		(3, 'Charlie', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read with default NULL handling (nil)
	df, err := goframe.FromSQL(db, "SELECT * FROM users ORDER BY id", nil)
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify NULL values are nil
	nameCol, _ := df.Select("name")
	if nameCol.Data[1] != nil {
		t.Errorf("Expected nil for NULL name, got %v", nameCol.Data[1])
	}

	ageCol, _ := df.Select("age")
	if ageCol.Data[2] != nil {
		t.Errorf("Expected nil for NULL age, got %v", ageCol.Data[2])
	}
}

// TestFromSQL_NullHandlerZero tests zero value NULL handling
func TestFromSQL_NullHandlerZero(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create table with nullable columns
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with NULLs
	_, err = db.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, NULL, 30),
		(3, 'Charlie', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read with zero value NULL handling
	df, err := goframe.FromSQL(db, "SELECT * FROM users ORDER BY id", nil,
		goframe.SQLReadOption{NullHandler: "zero"})
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify NULL values are zero values
	nameCol, _ := df.Select("name")
	if nameCol.Data[1] != "" {
		t.Errorf("Expected empty string for NULL name, got %v", nameCol.Data[1])
	}

	ageCol, _ := df.Select("age")
	if ageCol.Data[2] != int64(0) {
		t.Errorf("Expected 0 for NULL age, got %v", ageCol.Data[2])
	}
}

// TestFromSQL_NullHandlerSkipRow tests skip row NULL handling
func TestFromSQL_NullHandlerSkipRow(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create table with nullable columns
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with NULLs
	_, err = db.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, NULL, 30),
		(3, 'Charlie', NULL),
		(4, 'David', 40)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read with skip row NULL handling
	df, err := goframe.FromSQL(db, "SELECT * FROM users ORDER BY id", nil,
		goframe.SQLReadOption{NullHandler: "skip_row"})
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify only rows without NULLs are included
	if df.Nrows() != 2 {
		t.Errorf("Expected 2 rows (skipped 2 with NULLs), got %d", df.Nrows())
	}

	nameCol, _ := df.Select("name")
	if nameCol.Data[0] != "Alice" || nameCol.Data[1] != "David" {
		t.Errorf("Expected Alice and David, got %v and %v", nameCol.Data[0], nameCol.Data[1])
	}
}

// TestFromSQL_NullHandlerCustomMap tests custom map NULL handling
func TestFromSQL_NullHandlerCustomMap(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create table with nullable columns
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with NULLs
	_, err = db.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, NULL, 30),
		(3, 'Charlie', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read with custom NULL handling
	df, err := goframe.FromSQL(db, "SELECT * FROM users ORDER BY id", nil,
		goframe.SQLReadOption{
			NullHandler: map[string]any{
				"name": "Unknown",
				"age":  -1,
			},
		})
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify custom NULL values
	nameCol, _ := df.Select("name")
	if nameCol.Data[1] != "Unknown" {
		t.Errorf("Expected 'Unknown' for NULL name, got %v", nameCol.Data[1])
	}

	ageCol, _ := df.Select("age")
	if ageCol.Data[2] != -1 {
		t.Errorf("Expected -1 for NULL age, got %v", ageCol.Data[2])
	}
}

// TestFromSQL_TypeMapping tests SQL to Go type mapping
func TestFromSQL_TypeMapping(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create table with various types
	_, err := db.Exec(`
		CREATE TABLE types_test (
			int_col INTEGER,
			float_col REAL,
			text_col TEXT,
			bool_col INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO types_test (int_col, float_col, text_col, bool_col) VALUES
		(42, 3.14, 'test', 1)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read data
	df, err := goframe.FromSQL(db, "SELECT * FROM types_test", nil)
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify types
	intCol, _ := df.Select("int_col")
	if _, ok := intCol.Data[0].(int64); !ok {
		t.Errorf("Expected int64 type for INTEGER column, got %T", intCol.Data[0])
	}

	floatCol, _ := df.Select("float_col")
	if _, ok := floatCol.Data[0].(float64); !ok {
		t.Errorf("Expected float64 type for REAL column, got %T", floatCol.Data[0])
	}

	textCol, _ := df.Select("text_col")
	if _, ok := textCol.Data[0].(string); !ok {
		t.Errorf("Expected string type for TEXT column, got %T", textCol.Data[0])
	}
}

// TestFromSQL_ParameterizedQuery tests queries with arguments
func TestFromSQL_ParameterizedQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, 'Bob', 30),
		(3, 'Charlie', 35)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read with parameterized query
	df, err := goframe.FromSQL(db, "SELECT * FROM users WHERE age > ? ORDER BY id", []any{28})
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify results
	if df.Nrows() != 2 {
		t.Errorf("Expected 2 rows (age > 28), got %d", df.Nrows())
	}

	nameCol, _ := df.Select("name")
	if nameCol.Data[0] != "Bob" {
		t.Errorf("Expected 'Bob', got %v", nameCol.Data[0])
	}
}

// TestFromSQL_EmptyResult tests query with no results
func TestFromSQL_EmptyResult(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query empty table
	df, err := goframe.FromSQL(db, "SELECT * FROM users", nil)
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify empty DataFrame
	if df.Nrows() != 0 {
		t.Errorf("Expected 0 rows, got %d", df.Nrows())
	}

	// Verify columns exist even with no rows
	if len(df.ColumnNames()) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(df.ColumnNames()))
	}
}

// TestFromSQLTx_Transaction tests reading within a transaction
func TestFromSQLTx_Transaction(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Insert data in transaction
	_, err = tx.Exec(`
		INSERT INTO users (id, name, age) VALUES
		(1, 'Alice', 25),
		(2, 'Bob', 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read using FromSQLTx
	df, err := goframe.FromSQLTx(tx, "SELECT * FROM users ORDER BY id", nil)
	if err != nil {
		t.Fatalf("FromSQLTx failed: %v", err)
	}

	// Verify data
	if df.Nrows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.Nrows())
	}

	// Rollback - data should not persist
	tx.Rollback()

	// Verify data was rolled back
	df2, err := goframe.FromSQL(db, "SELECT * FROM users", nil)
	if err != nil {
		t.Fatalf("FromSQL after rollback failed: %v", err)
	}
	if df2.Nrows() != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", df2.Nrows())
	}
}

// TestFromSQLContext_Cancellation tests context cancellation
func TestFromSQLContext_Cancellation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Attempt to read with cancelled context
	_, err = goframe.FromSQLContext(ctx, db, "SELECT * FROM users", nil)
	if err == nil {
		t.Error("Expected error with cancelled context, got nil")
	}
}

// TestFromSQL_ErrorInvalidQuery tests error handling for invalid queries
func TestFromSQL_ErrorInvalidQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Try to query non-existent table
	_, err := goframe.FromSQL(db, "SELECT * FROM nonexistent_table", nil)
	if err == nil {
		t.Error("Expected error for invalid query, got nil")
	}
}

// TestFromSQL_ErrorClosedConnection tests error handling for closed connection
func TestFromSQL_ErrorClosedConnection(t *testing.T) {
	db := setupTestDB(t)
	db.Close() // Close immediately

	// Try to query with closed connection
	_, err := goframe.FromSQL(db, "SELECT 1", nil)
	if err == nil {
		t.Error("Expected error for closed connection, got nil")
	}
}

// TestFromSQLTxContext_TransactionWithContext tests transaction with context
func TestFromSQLTxContext_TransactionWithContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Begin transaction
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Read using FromSQLTxContext
	df, err := goframe.FromSQLTxContext(ctx, tx, "SELECT * FROM users", nil)
	if err != nil {
		t.Fatalf("FromSQLTxContext failed: %v", err)
	}

	// Verify data
	if df.Nrows() != 1 {
		t.Errorf("Expected 1 row, got %d", df.Nrows())
	}
}

// TestFromSQL_MultipleRowsLargeDataset tests reading a larger dataset
func TestFromSQL_MultipleRowsLargeDataset(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`
		CREATE TABLE large_dataset (
			id INTEGER,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert many rows
	tx, _ := db.Begin()
	for i := 0; i < 1000; i++ {
		_, err = tx.Exec("INSERT INTO large_dataset (id, value) VALUES (?, ?)", i, "value_"+string(rune(i)))
	}
	tx.Commit()

	// Read all data
	df, err := goframe.FromSQL(db, "SELECT * FROM large_dataset", nil)
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify row count
	if df.Nrows() != 1000 {
		t.Errorf("Expected 1000 rows, got %d", df.Nrows())
	}
}

// TestFromSQL_TimeType tests handling of TIME/DATE columns
func TestFromSQL_TimeType(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create table with timestamp
	_, err := db.Exec(`
		CREATE TABLE events (
			id INTEGER,
			event_time TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with current time
	now := time.Now()
	_, err = db.Exec("INSERT INTO events (id, event_time) VALUES (?, ?)", 1, now)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read data
	df, err := goframe.FromSQL(db, "SELECT * FROM events", nil)
	if err != nil {
		t.Fatalf("FromSQL failed: %v", err)
	}

	// Verify time column type
	timeCol, _ := df.Select("event_time")
	if _, ok := timeCol.Data[0].(time.Time); !ok {
		t.Errorf("Expected time.Time type for TIMESTAMP column, got %T", timeCol.Data[0])
	}
}
