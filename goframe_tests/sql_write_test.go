package goframe_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/kishyassin/goframe/dataframe"
)

// Helper functions (dialectTestCase, getDialects, setupMockDB) are defined in sql_read_test.go

// TestToSQL_CreateAndInsert tests basic write functionality for all dialects
func TestToSQL_CreateAndInsert(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("age", []int{25, 30, 35})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("salary", []float64{50000.0, 60000.0, 75000.0})))

			// Mock transaction Begin
			mock.ExpectBegin()

			// Mock table existence check (returns empty = table doesn't exist)
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT (3 rows, 3 columns = 9 values)
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 3))

			// Mock transaction Commit
			mock.ExpectCommit()

			// Write to database with explicit dialect
			err := df.ToSQL(db, "users", dataframe.SQLWriteOption{
				Dialect: dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQL failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_IfExistsFail tests that ToSQL fails if table already exists
func TestToSQL_IfExistsFail(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Mock Begin for first write
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// First write should succeed
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err != nil {
				t.Fatalf("First ToSQL failed: %v", err)
			}

			// Mock Begin for second write
			mock.ExpectBegin()

			// Mock table EXISTS
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("test_table"))

			// Mock Rollback (error path)
			mock.ExpectRollback()

			// Second write should fail with IfExists="fail" (default)
			err = df.ToSQL(db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err == nil {
				t.Error("Expected error when writing to existing table with IfExists='fail', got nil")
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_IfExistsReplace tests DROP and CREATE behavior
func TestToSQL_IfExistsReplace(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create replacement DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{10, 20})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("value", []float64{1.5, 2.5})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table EXISTS
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("test_table"))

			// Mock DROP TABLE
			mock.ExpectExec("DROP TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Replace the table
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
				IfExists: "replace",
				Dialect:  dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQL with IfExists='replace' failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_IfExistsAppend tests appending to existing table
func TestToSQL_IfExistsAppend(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame to append
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{3, 4})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{"Charlie", "Dave"})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table EXISTS
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("test_table"))

			// Mock INSERT (no CREATE TABLE for append)
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Append to the table
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
				IfExists: "append",
				Dialect:  dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQL with IfExists='append' failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_EmptyDataFrame tests writing an empty DataFrame for all dialects
func TestToSQL_EmptyDataFrame(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create empty DataFrame with schema
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// No INSERT expected for empty DataFrame

			// Mock Commit
			mock.ExpectCommit()

			// Write to database
			err := df.ToSQL(db, "empty_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err != nil {
				t.Fatalf("ToSQL with empty DataFrame failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_WithNilValues tests handling of nil values for all dialects
func TestToSQL_WithNilValues(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame with nil values
			df := dataframe.NewDataFrame()
			nameData := []any{"Alice", nil, "Charlie"}
			ageData := []any{25, 30, nil}
			df.AddColumn(dataframe.NewColumn("name", nameData))
			df.AddColumn(dataframe.NewColumn("age", ageData))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT (3 rows with nil values handled via sql.Null* types)
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 3))

			// Mock Commit
			mock.ExpectCommit()

			// Write to database
			err := df.ToSQL(db, "users_with_nulls", dataframe.SQLWriteOption{
				Dialect: dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQL failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_AllTypes tests all supported data types for all dialects
func TestToSQL_AllTypes(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame with all types
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("col_int", []int{1, 2})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("col_int64", []int64{100, 200})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("col_float64", []float64{1.5, 2.5})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("col_string", []string{"hello", "world"})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("col_bool", []bool{true, false})))

			now := time.Now().Truncate(time.Second)
			timeData := []any{now, now.Add(24 * time.Hour)}
			df.AddColumn(dataframe.NewColumn("col_time", timeData))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE (6 columns: int, int64, float64, string, bool, time)
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT (2 rows, 6 columns each)
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Write to database
			err := df.ToSQL(db, "all_types", dataframe.SQLWriteOption{
				Dialect: dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQL failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_CustomTypeMap tests custom type mapping for all dialects
func TestToSQL_CustomTypeMap(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("email", []string{"alice@example.com", "bob@example.com"})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE with custom types
			// The CREATE TABLE should use the custom TypeMap values
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Write with custom type map
			err := df.ToSQL(db, "users", dataframe.SQLWriteOption{
				Dialect: dialect.name,
				TypeMap: map[string]string{
					"id":    "INTEGER PRIMARY KEY",
					"email": "VARCHAR(255)",
				},
			})
			if err != nil {
				t.Fatalf("ToSQL with TypeMap failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_BatchInsert tests batch insertion with large dataset for all dialects
func TestToSQL_BatchInsert(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create large DataFrame (5000 rows)
			const numRows = 5000
			const batchSize = 1000
			ids := make([]int, numRows)
			values := make([]float64, numRows)
			for i := 0; i < numRows; i++ {
				ids[i] = i + 1
				values[i] = float64(i) * 1.5
			}

			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", ids)))
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("value", values)))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock 5 batch INSERTs (5000 rows / 1000 batch size = 5 batches)
			for i := 0; i < 5; i++ {
				mock.ExpectExec("INSERT INTO").
					WillReturnResult(sqlmock.NewResult(0, batchSize))
			}

			// Mock Commit
			mock.ExpectCommit()

			// Write with custom batch size
			err := df.ToSQL(db, "large_table", dataframe.SQLWriteOption{
				Dialect:   dialect.name,
				BatchSize: batchSize,
			})
			if err != nil {
				t.Fatalf("ToSQL with large dataset failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQLContext tests context support for all dialects
func TestToSQLContext(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Write with context
			ctx := context.Background()
			err := df.ToSQLContext(ctx, db, "test_table", dataframe.SQLWriteOption{
				Dialect: dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQLContext failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQLTx tests transaction support for all dialects
func TestToSQLTx(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrames
			df1 := dataframe.NewDataFrame()
			df1.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			df2 := dataframe.NewDataFrame()
			df2.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{3, 4})))

			// Mock Begin transaction
			mock.ExpectBegin()

			// Mock operations for first DataFrame (table1)
			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))
			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))
			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock operations for second DataFrame (table2)
			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))
			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))
			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Begin transaction
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Write both DataFrames in same transaction
			err = df1.ToSQLTx(tx, "table1", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err != nil {
				tx.Rollback()
				t.Fatalf("First ToSQLTx failed: %v", err)
			}

			err = df2.ToSQLTx(tx, "table2", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err != nil {
				tx.Rollback()
				t.Fatalf("Second ToSQLTx failed: %v", err)
			}

			// Commit transaction
			if err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQLTx_Rollback tests transaction rollback for all dialects
func TestToSQLTx_Rollback(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Mock Begin transaction
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Rollback
			mock.ExpectRollback()

			// Begin transaction
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Write data
			err = df.ToSQLTx(tx, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err != nil {
				tx.Rollback()
				t.Fatalf("ToSQLTx failed: %v", err)
			}

			// Rollback instead of commit
			tx.Rollback()

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_InvalidOptions tests error handling for invalid options (validation happens before DB operations)
func TestToSQL_InvalidOptions(t *testing.T) {
	db, mock := setupMockDB(t)
	defer db.Close()

	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1})))

	// Test invalid IfExists
	// Mock Begin and Rollback (in case validation happens after transaction start)
	mock.ExpectBegin()
	mock.ExpectRollback()
	err := df.ToSQL(db, "test", dataframe.SQLWriteOption{IfExists: "invalid"})
	if err == nil {
		t.Error("Expected error for invalid IfExists option, got nil")
	}

	// Test invalid BatchSize (negative)
	mock.ExpectBegin()
	mock.ExpectRollback()
	err = df.ToSQL(db, "test", dataframe.SQLWriteOption{BatchSize: -1})
	if err == nil {
		t.Error("Expected error for negative BatchSize, got nil")
	}

	// Test invalid Dialect
	mock.ExpectBegin()
	mock.ExpectRollback()
	err = df.ToSQL(db, "test", dataframe.SQLWriteOption{Dialect: "invalid"})
	if err == nil {
		t.Error("Expected error for invalid Dialect, got nil")
	}
}

// TestToSQL_ExplicitDialect tests specifying dialect explicitly for all dialects
func TestToSQL_ExplicitDialect(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT
			mock.ExpectExec("INSERT INTO").
				WillReturnResult(sqlmock.NewResult(0, 2))

			// Mock Commit
			mock.ExpectCommit()

			// Write with explicit dialect
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
				Dialect: dialect.name,
			})
			if err != nil {
				t.Fatalf("ToSQL with explicit dialect failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// ===== NEW ERROR/NEGATIVE TESTS WITH SQLMOCK =====

// TestToSQL_ErrorContextCancellation tests context cancellation for all dialects
func TestToSQL_ErrorContextCancellation(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			// Mock Begin (may or may not be called depending on timing)
			mock.ExpectBegin().WillReturnError(context.Canceled)

			// Attempt to write with cancelled context
			err := df.ToSQLContext(ctx, db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})
	}
}

// TestToSQL_ErrorContextTimeout tests context timeout for all dialects
func TestToSQL_ErrorContextTimeout(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Create context with very short timeout
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
			defer cancel()

			// Wait for timeout
			time.Sleep(2 * time.Millisecond)

			// Mock Begin with timeout error
			mock.ExpectBegin().WillReturnError(context.DeadlineExceeded)

			// Attempt to write with timed-out context
			err := df.ToSQLContext(ctx, db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err == nil {
				t.Error("Expected error with timed-out context, got nil")
			}
		})
	}
}

// TestToSQL_ErrorClosedConnection tests writing to closed database for all dialects
func TestToSQL_ErrorClosedConnection(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1})))

			// Mock Begin returning connection error
			mock.ExpectBegin().WillReturnError(sql.ErrConnDone)

			// Close the database
			mock.ExpectClose()
			db.Close()

			// Try to write to closed connection
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err == nil {
				t.Error("Expected error for closed connection, got nil")
			}
		})
	}
}

// TestToSQL_ErrorTableCreationFails tests CREATE TABLE failure for all dialects
func TestToSQL_ErrorTableCreationFails(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE failure
			mock.ExpectExec("CREATE TABLE").
				WillReturnError(sql.ErrConnDone)

			// Mock Rollback
			mock.ExpectRollback()

			// Attempt to write - should fail on CREATE TABLE
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err == nil {
				t.Error("Expected error when CREATE TABLE fails, got nil")
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_ErrorInsertFails tests INSERT failure for all dialects
func TestToSQL_ErrorInsertFails(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create test DataFrame
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE success
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock INSERT failure
			mock.ExpectExec("INSERT INTO").
				WillReturnError(sql.ErrTxDone)

			// Mock Rollback
			mock.ExpectRollback()

			// Attempt to write - should fail on INSERT
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{Dialect: dialect.name})
			if err == nil {
				t.Error("Expected error when INSERT fails, got nil")
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_ErrorInvalidDialect tests error handling for invalid dialect
func TestToSQL_ErrorInvalidDialect(t *testing.T) {
	db, mock := setupMockDB(t)
	defer db.Close()

	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1})))

	// Mock Begin (may not be reached due to validation)
	mock.ExpectBegin()
	mock.ExpectRollback()

	// Try with invalid dialect
	err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{Dialect: "invalid_dialect"})
	if err == nil {
		t.Error("Expected error for invalid dialect, got nil")
	}
}

// TestToSQL_BatchSizeOne tests batch size of 1 for all dialects
func TestToSQL_BatchSizeOne(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame with 3 rows
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2, 3})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock 3 separate INSERTs (batch size = 1)
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))

			// Mock Commit
			mock.ExpectCommit()

			// Write with batch size 1
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
				Dialect:   dialect.name,
				BatchSize: 1,
			})
			if err != nil {
				t.Fatalf("ToSQL with BatchSize=1 failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_BatchSizeExactMatch tests batch size exactly matching row count
func TestToSQL_BatchSizeExactMatch(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame with exactly 10 rows
			ids := make([]int, 10)
			for i := 0; i < 10; i++ {
				ids[i] = i + 1
			}
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", ids)))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock single INSERT (all 10 rows in one batch)
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 10))

			// Mock Commit
			mock.ExpectCommit()

			// Write with batch size = row count
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
				Dialect:   dialect.name,
				BatchSize: 10,
			})
			if err != nil {
				t.Fatalf("ToSQL with exact batch size failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestToSQL_BatchSizeLargerThanData tests batch size larger than row count
func TestToSQL_BatchSizeLargerThanData(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create DataFrame with 5 rows
			df := dataframe.NewDataFrame()
			df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2, 3, 4, 5})))

			// Mock Begin
			mock.ExpectBegin()

			// Mock table doesn't exist
			mock.ExpectQuery("SELECT (.+) FROM (.+)").
				WillReturnRows(sqlmock.NewRows([]string{"name"}))

			// Mock CREATE TABLE
			mock.ExpectExec("CREATE TABLE").
				WillReturnResult(sqlmock.NewResult(0, 0))

			// Mock single INSERT (all 5 rows)
			mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 5))

			// Mock Commit
			mock.ExpectCommit()

			// Write with batch size > row count
			err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
				Dialect:   dialect.name,
				BatchSize: 1000,
			})
			if err != nil {
				t.Fatalf("ToSQL with large batch size failed: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}
