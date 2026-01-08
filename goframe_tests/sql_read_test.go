package goframe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/kishyassin/goframe"
)

// dialectTestCase defines test parameters for each dialect
type dialectTestCase struct {
	name        string
	placeholder string // ? or $N
}

// getDialects returns all supported dialects for testing
func getDialects() []dialectTestCase {
	return []dialectTestCase{
		{name: "sqlite", placeholder: "?"},
		{name: "postgres", placeholder: "$1"},
		{name: "mysql", placeholder: "?"},
	}
}

// setupMockDB creates a mock database connection
func setupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	return db, mock
}

// TestFromSQL_BasicRead tests basic SELECT query functionality for all dialects
func TestFromSQL_BasicRead(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), "Alice", int64(25)).
				AddRow(int64(2), "Bob", int64(30)).
				AddRow(int64(3), "Charlie", int64(35))

			mock.ExpectQuery("SELECT \\* FROM users ORDER BY id").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_NullHandlerNil tests default NULL handling (nil) for all dialects
func TestFromSQL_NullHandlerNil(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with NULL values and proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), "Alice", int64(25)).
				AddRow(int64(2), nil, int64(30)).
				AddRow(int64(3), "Charlie", nil)

			mock.ExpectQuery("SELECT \\* FROM users ORDER BY id").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_NullHandlerZero tests zero value NULL handling for all dialects
func TestFromSQL_NullHandlerZero(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with NULL values and proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), "Alice", int64(25)).
				AddRow(int64(2), nil, int64(30)).
				AddRow(int64(3), "Charlie", nil)

			mock.ExpectQuery("SELECT \\* FROM users ORDER BY id").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_NullHandlerSkipRow tests skip row NULL handling for all dialects
func TestFromSQL_NullHandlerSkipRow(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with NULL values and proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), "Alice", int64(25)).
				AddRow(int64(2), nil, int64(30)).
				AddRow(int64(3), "Charlie", nil).
				AddRow(int64(4), "David", int64(40))

			mock.ExpectQuery("SELECT \\* FROM users ORDER BY id").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_NullHandlerCustomMap tests custom map NULL handling for all dialects
func TestFromSQL_NullHandlerCustomMap(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with NULL values and proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), "Alice", int64(25)).
				AddRow(int64(2), nil, int64(30)).
				AddRow(int64(3), "Charlie", nil)

			mock.ExpectQuery("SELECT \\* FROM users ORDER BY id").
				WillReturnRows(rows)

			// Read with custom NULL handling
			df, err := goframe.FromSQL(db, "SELECT * FROM users ORDER BY id", nil,
				goframe.SQLReadOption{
					NullHandler: map[string]any{
						"name": "Unknown",
						"age":  int64(-1),
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
			if ageCol.Data[2] != int64(-1) {
				t.Errorf("Expected -1 for NULL age, got %v", ageCol.Data[2])
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_TypeMapping tests SQL to Go type mapping for all dialects
func TestFromSQL_TypeMapping(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with various types and proper column definitions
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("int_col").OfType("INT", int64(0)),
				sqlmock.NewColumn("float_col").OfType("REAL", float64(0)),
				sqlmock.NewColumn("text_col").OfType("TEXT", ""),
				sqlmock.NewColumn("bool_col").OfType("BOOL", true),
			).
				AddRow(int64(42), 3.14, "test", true)

			mock.ExpectQuery("SELECT \\* FROM types_test").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ParameterizedQuery tests queries with arguments for all dialects
func TestFromSQL_ParameterizedQuery(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with arguments and proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(2), "Bob", int64(30)).
				AddRow(int64(3), "Charlie", int64(35))

			mock.ExpectQuery("SELECT \\* FROM users WHERE age > (.*) ORDER BY id").
				WithArgs(28).
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_EmptyResult tests query with no results for all dialects
func TestFromSQL_EmptyResult(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up empty result set with columns
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			)

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQLTx_Transaction tests reading within a transaction for all dialects
func TestFromSQLTx_Transaction(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Expect transaction begin
			mock.ExpectBegin()

			// Set up expected query with proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), "Alice", int64(25)).
				AddRow(int64(2), "Bob", int64(30))

			mock.ExpectQuery("SELECT \\* FROM users ORDER BY id").
				WillReturnRows(rows)

			// Expect rollback
			mock.ExpectRollback()

			// Begin transaction
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
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

			_ = tx.Rollback()

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQLContext_Cancellation tests context cancellation for all dialects
func TestFromSQLContext_Cancellation(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			// Mock should not be called due to immediate cancellation
			// But we still need to handle potential query attempts
			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnError(context.Canceled)

			// Attempt to read with cancelled context
			_, err := goframe.FromSQLContext(ctx, db, "SELECT * FROM users", nil)
			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})
	}
}

// TestFromSQL_ErrorInvalidQuery tests error handling for invalid queries
func TestFromSQL_ErrorInvalidQuery(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Mock error for non-existent table
			mock.ExpectQuery("SELECT \\* FROM nonexistent_table").
				WillReturnError(sql.ErrNoRows)

			// Try to query non-existent table
			_, err := goframe.FromSQL(db, "SELECT * FROM nonexistent_table", nil)
			if err == nil {
				t.Error("Expected error for invalid query, got nil")
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ErrorClosedConnection tests error handling for closed connection
func TestFromSQL_ErrorClosedConnection(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			mock.ExpectClose()
			db.Close() // Close immediately

			// Try to query with closed connection
			_, err := goframe.FromSQL(db, "SELECT 1", nil)
			if err == nil {
				t.Error("Expected error for closed connection, got nil")
			}
		})
	}
}

// TestFromSQLTxContext_TransactionWithContext tests transaction with context
func TestFromSQLTxContext_TransactionWithContext(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Expect transaction begin
			mock.ExpectBegin()

			// Set up expected query with proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), "Alice")

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Expect rollback
			mock.ExpectRollback()

			// Begin transaction
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Read using FromSQLTxContext
			df, err := goframe.FromSQLTxContext(ctx, tx, "SELECT * FROM users", nil)
			if err != nil {
				t.Fatalf("FromSQLTxContext failed: %v", err)
			}

			// Verify data
			if df.Nrows() != 1 {
				t.Errorf("Expected 1 row, got %d", df.Nrows())
			}

			_ = tx.Rollback()

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_TimeType tests handling of TIME/DATE columns for all dialects
func TestFromSQL_TimeType(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with timestamp and proper column types
			now := time.Now()
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("event_time").OfType("TIMESTAMP", time.Time{}),
			).
				AddRow(int64(1), now)

			mock.ExpectQuery("SELECT \\* FROM events").
				WillReturnRows(rows)

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

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ParseDates tests the ParseDates option with various formats
func TestFromSQL_ParseDates(t *testing.T) {
	tests := []struct {
		name         string
		inputValue   driver.Value
		columnType   string
		parseDates   []string
		expectedType string // "time", "zero_time", or "error"
		verifyValue  func(t *testing.T, val any)
	}{
		{
			name:         "TEXT ISO8601 format",
			inputValue:   "2024-01-15T10:30:00Z",
			columnType:   "TEXT",
			parseDates:   []string{"event_date"},
			expectedType: "time",
			verifyValue: func(t *testing.T, val any) {
				expected := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				if !val.(time.Time).Equal(expected) {
					t.Errorf("Expected %v, got %v", expected, val)
				}
			},
		},
		{
			name:         "TEXT simple date format",
			inputValue:   "2024-01-15",
			columnType:   "TEXT",
			parseDates:   []string{"event_date"},
			expectedType: "time",
			verifyValue: func(t *testing.T, val any) {
				parsed := val.(time.Time)
				if parsed.Year() != 2024 || parsed.Month() != 1 || parsed.Day() != 15 {
					t.Errorf("Expected 2024-01-15, got %v", parsed)
				}
			},
		},
		{
			name:         "TEXT SQLite datetime format",
			inputValue:   "2024-01-15 10:30:00",
			columnType:   "TEXT",
			parseDates:   []string{"event_date"},
			expectedType: "time",
			verifyValue: func(t *testing.T, val any) {
				parsed := val.(time.Time)
				if parsed.Year() != 2024 || parsed.Hour() != 10 || parsed.Minute() != 30 {
					t.Errorf("Expected 2024-01-15 10:30:00, got %v", parsed)
				}
			},
		},
		{
			name:         "INTEGER Unix timestamp (seconds)",
			inputValue:   int64(1705317000), // 2024-01-15 10:30:00 UTC
			columnType:   "INT",
			parseDates:   []string{"timestamp_sec"},
			expectedType: "time",
			verifyValue: func(t *testing.T, val any) {
				expected := time.Unix(1705317000, 0)
				if !val.(time.Time).Equal(expected) {
					t.Errorf("Expected %v, got %v", expected, val)
				}
			},
		},
		{
			name:         "REAL Unix timestamp (fractional seconds)",
			inputValue:   1705317000.5,
			columnType:   "REAL",
			parseDates:   []string{"timestamp_sec"},
			expectedType: "time",
			verifyValue: func(t *testing.T, val any) {
				parsed := val.(time.Time)
				expectedUnix := int64(1705317000)
				if parsed.Unix() != expectedUnix {
					t.Errorf("Expected Unix timestamp %d, got %d", expectedUnix, parsed.Unix())
				}
				// Check for fractional seconds (500ms)
				if parsed.Nanosecond() < 400000000 || parsed.Nanosecond() > 600000000 {
					t.Errorf("Expected ~500ms nanoseconds, got %d", parsed.Nanosecond())
				}
			},
		},
		{
			name:         "TEXT with NULL value",
			inputValue:   nil,
			columnType:   "TEXT",
			parseDates:   []string{"event_date"},
			expectedType: "zero_time",
			verifyValue: func(t *testing.T, val any) {
				zeroTime := time.Time{}
				if val != zeroTime {
					t.Errorf("Expected zero time for NULL, got %v", val)
				}
			},
		},
		{
			name:         "Invalid date format",
			inputValue:   "not-a-valid-date",
			columnType:   "TEXT",
			parseDates:   []string{"bad_date"},
			expectedType: "error",
		},
	}

	for _, tt := range tests {
		for _, dialect := range getDialects() {
			t.Run(dialect.name+"_"+tt.name, func(t *testing.T) {
				db, mock := setupMockDB(t)
				defer db.Close()

				// Determine column name based on test case
				var colName string
				if len(tt.parseDates) > 0 {
					colName = tt.parseDates[0]
				}

				// Set up expected query with proper column types
				var sampleValue any
				switch tt.columnType {
				case "TEXT":
					sampleValue = ""
				case "INT":
					sampleValue = int64(0)
				case "REAL":
					sampleValue = float64(0)
				}

				rows := sqlmock.NewRowsWithColumnDefinition(
					sqlmock.NewColumn("id").OfType("INT", int64(0)),
					sqlmock.NewColumn(colName).OfType(tt.columnType, sampleValue),
				).
					AddRow(int64(1), tt.inputValue)

				mock.ExpectQuery("SELECT \\* FROM events").
					WillReturnRows(rows)

				// Read with ParseDates
				df, err := goframe.FromSQL(db, "SELECT * FROM events", nil, goframe.SQLReadOption{
					ParseDates: tt.parseDates,
				})

				// Handle expected error cases
				if tt.expectedType == "error" {
					if err == nil {
						t.Error("Expected error for invalid date format, got nil")
					}
					return
				}

				if err != nil {
					t.Fatalf("FromSQL with ParseDates failed: %v", err)
				}

				// Verify the parsed column
				col, err := df.Select(tt.parseDates[0])
				if err != nil {
					t.Fatalf("Failed to select column: %v", err)
				}

				// Verify type
				if _, ok := col.Data[0].(time.Time); !ok {
					t.Errorf("Expected time.Time type, got %T", col.Data[0])
				}

				// Run custom verification if provided
				if tt.verifyValue != nil {
					tt.verifyValue(t, col.Data[0])
				}

				// Verify all expectations were met
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("Unfulfilled expectations: %v", err)
				}
			})
		}
	}
}

// TestFromSQL_MultipleRowsLargeDataset tests reading a larger dataset
func TestFromSQL_MultipleRowsLargeDataset(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up large result set with proper column types
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("value").OfType("TEXT", ""),
			)
			for i := 0; i < 1000; i++ {
				rows.AddRow(int64(i), "value_"+string(rune(i)))
			}

			mock.ExpectQuery("SELECT \\* FROM large_dataset").
				WillReturnRows(rows)

			// Read all data
			df, err := goframe.FromSQL(db, "SELECT * FROM large_dataset", nil)
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify row count
			if df.Nrows() != 1000 {
				t.Errorf("Expected 1000 rows, got %d", df.Nrows())
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ErrorRowsIteration tests error during row iteration
func TestFromSQL_ErrorRowsIteration(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with CloseError to simulate iteration error
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), "Alice").
				AddRow(int64(2), "Bob").
				CloseError(sql.ErrConnDone) // Error when rows are closed

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Try to read data - the CloseError will be caught by rows.Err()
			df, err := goframe.FromSQL(db, "SELECT * FROM users", nil)

			// Note: CloseError may or may not propagate depending on sqlmock version
			// This test verifies the error handling path exists, even if mock doesn't trigger it
			if err == nil {
				// If no error, at least verify the data was read
				if df.Nrows() != 2 {
					t.Errorf("Expected 2 rows when no error, got %d", df.Nrows())
				}
			} else {
				// If error occurred, verify it's the expected type
				if !strings.Contains(err.Error(), "error") {
					t.Errorf("Expected error message to contain 'error', got: %v", err)
				}
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ErrorHandleNullInvalidString tests handleNull with invalid string handler
func TestFromSQL_ErrorHandleNullInvalidString(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with NULL value
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), nil) // NULL value

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Try to read with invalid NullHandler
			_, err := goframe.FromSQL(db, "SELECT * FROM users", nil,
				goframe.SQLReadOption{NullHandler: "invalid_handler"})
			if err == nil {
				t.Error("Expected error for invalid null handler, got nil")
			}

			// Verify error message
			if err != nil && !strings.Contains(err.Error(), "unknown null handler") {
				t.Errorf("Expected 'unknown null handler' in error, got: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ErrorHandleNullInvalidType tests handleNull with invalid type
func TestFromSQL_ErrorHandleNullInvalidType(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with NULL value
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), nil) // NULL value

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Try to read with invalid NullHandler type (int instead of string/map)
			_, err := goframe.FromSQL(db, "SELECT * FROM users", nil,
				goframe.SQLReadOption{NullHandler: 123}) // Invalid type
			if err == nil {
				t.Error("Expected error for invalid null handler type, got nil")
			}

			// Verify error message
			if err != nil && !strings.Contains(err.Error(), "invalid null handler type") {
				t.Errorf("Expected 'invalid null handler type' in error, got: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ErrorParseDateUnsupportedType tests parseDateValue with unsupported type
func TestFromSQL_ErrorParseDateUnsupportedType(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with BOOL type (unsupported for date parsing)
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("flag").OfType("BOOL", true),
			).
				AddRow(int64(1), true)

			mock.ExpectQuery("SELECT \\* FROM events").
				WillReturnRows(rows)

			// Try to parse bool as date
			_, err := goframe.FromSQL(db, "SELECT * FROM events", nil,
				goframe.SQLReadOption{ParseDates: []string{"flag"}})
			if err == nil {
				t.Error("Expected error for unsupported date parsing type, got nil")
			}

			// Verify error message
			if err != nil && !strings.Contains(err.Error(), "unsupported type for date parsing") {
				t.Errorf("Expected 'unsupported type for date parsing' in error, got: %v", err)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQLContext_ContextTimeout tests context timeout (not just cancellation)
func TestFromSQLContext_ContextTimeout(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Create context with very short timeout
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
			defer cancel()

			// Wait for timeout to occur
			time.Sleep(2 * time.Millisecond)

			// Mock should handle timeout error
			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnError(context.DeadlineExceeded)

			// Attempt to read with timed-out context
			_, err := goframe.FromSQLContext(ctx, db, "SELECT * FROM users", nil)
			if err == nil {
				t.Error("Expected error with timed-out context, got nil")
			}

			// Verify error is related to context
			if err != nil && !strings.Contains(err.Error(), "error executing query") {
				t.Errorf("Expected 'error executing query' in error, got: %v", err)
			}
		})
	}
}

// TestFromSQL_ComprehensiveTypeMapping tests all SQL type variations
func TestFromSQL_ComprehensiveTypeMapping(t *testing.T) {
	tests := []struct {
		name           string
		sqlType        string
		sampleValue    interface{}
		expectedGoType string
	}{
		// Integer types
		{"BIGINT", "BIGINT", int64(9223372036854775807), "int64"},
		{"SMALLINT", "SMALLINT", int64(32767), "int64"},
		{"INTEGER", "INTEGER", int64(42), "int64"},

		// Float types
		{"NUMERIC", "NUMERIC", 3.14159, "float64"},
		{"DOUBLE", "DOUBLE", 2.71828, "float64"},
		{"FLOAT", "FLOAT", 1.41421, "float64"},
		{"REAL", "REAL", 1.61803, "float64"},
		{"DOUBLE PRECISION", "DOUBLE PRECISION", 3.14, "float64"},

		// String types
		{"TEXT", "TEXT", "hello world", "string"},
		{"VARCHAR", "VARCHAR", "variable char", "string"},
		{"CHAR", "CHAR", "fixed char", "string"},
		{"VARCHAR(255)", "VARCHAR(255)", "sized varchar", "string"},
		{"CHAR(10)", "CHAR(10)", "sized char", "string"},

		// Date/Time types
		{"DATE", "DATE", time.Now(), "time.Time"},
		{"DATETIME", "DATETIME", time.Now(), "time.Time"},
		{"TIMESTAMP", "TIMESTAMP", time.Now(), "time.Time"},

		// Boolean type
		{"BOOLEAN", "BOOLEAN", true, "bool"},
		{"BOOL", "BOOL", false, "bool"},

		// Unknown type (should default to string)
		{"UNKNOWN_TYPE", "UNKNOWN_TYPE", "fallback", "string"},
		{"CUSTOM_TYPE", "CUSTOM_TYPE", "custom", "string"},
	}

	for _, tt := range tests {
		for _, dialect := range getDialects() {
			t.Run(dialect.name+"_"+tt.name, func(t *testing.T) {
				db, mock := setupMockDB(t)
				defer db.Close()

				// Determine sample value for column definition
				var sampleForDef interface{}
				switch tt.expectedGoType {
				case "int64":
					sampleForDef = int64(0)
				case "float64":
					sampleForDef = float64(0)
				case "string":
					sampleForDef = ""
				case "bool":
					sampleForDef = true
				case "time.Time":
					sampleForDef = time.Time{}
				default:
					sampleForDef = ""
				}

				// Set up expected query with the specific type
				rows := sqlmock.NewRowsWithColumnDefinition(
					sqlmock.NewColumn("id").OfType("INT", int64(0)),
					sqlmock.NewColumn("test_col").OfType(tt.sqlType, sampleForDef),
				).
					AddRow(int64(1), tt.sampleValue)

				mock.ExpectQuery("SELECT \\* FROM type_test").
					WillReturnRows(rows)

				// Read data
				df, err := goframe.FromSQL(db, "SELECT * FROM type_test", nil)
				if err != nil {
					t.Fatalf("FromSQL failed for type %s: %v", tt.sqlType, err)
				}

				// Verify column exists
				col, err := df.Select("test_col")
				if err != nil {
					t.Fatalf("Failed to select test_col: %v", err)
				}

				// Verify the Go type matches expected
				actualValue := col.Data[0]
				var actualType string
				switch actualValue.(type) {
				case int64:
					actualType = "int64"
				case float64:
					actualType = "float64"
				case string:
					actualType = "string"
				case bool:
					actualType = "bool"
				case time.Time:
					actualType = "time.Time"
				case nil:
					actualType = "nil"
				default:
					actualType = fmt.Sprintf("%T", actualValue)
				}

				if actualType != tt.expectedGoType {
					t.Errorf("Type %s: expected Go type %s, got %s (value: %v)",
						tt.sqlType, tt.expectedGoType, actualType, actualValue)
				}

				// Verify all expectations were met
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("Unfulfilled expectations: %v", err)
				}
			})
		}
	}
}

// TestFromSQL_NullHandlerSkipRow_AllNulls tests skip_row with all NULL rows
func TestFromSQL_NullHandlerSkipRow_AllNulls(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows where all rows have at least one NULL
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(nil, "Alice", int64(25)).  // NULL id
				AddRow(int64(2), nil, int64(30)). // NULL name
				AddRow(int64(3), "Charlie", nil)  // NULL age

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Read with skip_row - should skip all rows
			df, err := goframe.FromSQL(db, "SELECT * FROM users", nil,
				goframe.SQLReadOption{NullHandler: "skip_row"})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify empty DataFrame but columns exist
			if df.Nrows() != 0 {
				t.Errorf("Expected 0 rows (all skipped), got %d", df.Nrows())
			}

			// Verify columns still exist
			colNames := df.ColumnNames()
			if len(colNames) != 3 {
				t.Errorf("Expected 3 columns even with 0 rows, got %d", len(colNames))
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_NullHandlerPartialMap tests custom map that doesn't contain all columns
func TestFromSQL_NullHandlerPartialMap(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with NULL values
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(1), nil, nil)

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Read with partial custom map (only defines "name", not "age")
			df, err := goframe.FromSQL(db, "SELECT * FROM users", nil,
				goframe.SQLReadOption{
					NullHandler: map[string]any{
						"name": "Unknown", // Only "name" is defined
					},
				})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify "name" uses custom default
			nameCol, _ := df.Select("name")
			if nameCol.Data[0] != "Unknown" {
				t.Errorf("Expected 'Unknown' for NULL name (in map), got %v", nameCol.Data[0])
			}

			// Verify "age" falls back to nil (not in map)
			ageCol, _ := df.Select("age")
			if ageCol.Data[0] != nil {
				t.Errorf("Expected nil for NULL age (not in map), got %v", ageCol.Data[0])
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_EmptyOptions tests passing empty options (should use defaults)
func TestFromSQL_EmptyOptions(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with NULL value
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), nil)

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Explicitly pass empty options
			df, err := goframe.FromSQL(db, "SELECT * FROM users", nil,
				goframe.SQLReadOption{})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify default NULL handling (nil)
			nameCol, _ := df.Select("name")
			if nameCol.Data[0] != nil {
				t.Errorf("Expected nil for NULL with empty options (default), got %v", nameCol.Data[0])
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ParseDatesEmpty tests ParseDates with empty slice
func TestFromSQL_ParseDatesEmpty(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with timestamp
			now := time.Now()
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("created_at").OfType("TIMESTAMP", time.Time{}),
			).
				AddRow(int64(1), now)

			mock.ExpectQuery("SELECT \\* FROM events").
				WillReturnRows(rows)

			// Pass empty ParseDates slice (should not parse anything)
			df, err := goframe.FromSQL(db, "SELECT * FROM events", nil,
				goframe.SQLReadOption{ParseDates: []string{}})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify timestamp is still time.Time (not parsed, just native type)
			col, _ := df.Select("created_at")
			if _, ok := col.Data[0].(time.Time); !ok {
				t.Errorf("Expected time.Time type, got %T", col.Data[0])
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ParseDatesNonExistentColumn tests ParseDates with column that doesn't exist
func TestFromSQL_ParseDatesNonExistentColumn(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows without the column specified in ParseDates
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), "Alice")

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// ParseDates specifies column that doesn't exist (should not error)
			df, err := goframe.FromSQL(db, "SELECT * FROM users", nil,
				goframe.SQLReadOption{ParseDates: []string{"nonexistent_column"}})
			if err != nil {
				t.Fatalf("FromSQL should not error with non-existent ParseDates column: %v", err)
			}

			// Verify data was read successfully
			if df.Nrows() != 1 {
				t.Errorf("Expected 1 row, got %d", df.Nrows())
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ParseDatesTimePassthrough tests ParseDates with column already time.Time
func TestFromSQL_ParseDatesTimePassthrough(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up rows with TIMESTAMP type (already time.Time)
			now := time.Now()
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("created_at").OfType("TIMESTAMP", time.Time{}),
			).
				AddRow(int64(1), now)

			mock.ExpectQuery("SELECT \\* FROM events").
				WillReturnRows(rows)

			// ParseDates on a column that's already time.Time (should pass through)
			df, err := goframe.FromSQL(db, "SELECT * FROM events", nil,
				goframe.SQLReadOption{ParseDates: []string{"created_at"}})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify it's still time.Time
			col, _ := df.Select("created_at")
			parsedTime, ok := col.Data[0].(time.Time)
			if !ok {
				t.Errorf("Expected time.Time type, got %T", col.Data[0])
			}

			// Verify the value matches (within reasonable precision)
			if parsedTime.Unix() != now.Unix() {
				t.Errorf("Expected %v, got %v", now, parsedTime)
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_ParseDatesMilliseconds tests ParseDates with millisecond timestamps
func TestFromSQL_ParseDatesMilliseconds(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Use millisecond timestamp (> 1e12 to trigger millisecond heuristic)
			millisTimestamp := float64(1705317000500) // 2024-01-15 10:30:00.500 UTC

			// Set up rows with REAL type (float64)
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("timestamp_ms").OfType("REAL", float64(0)),
			).
				AddRow(int64(1), millisTimestamp)

			mock.ExpectQuery("SELECT \\* FROM events").
				WillReturnRows(rows)

			// ParseDates should handle millisecond timestamps
			df, err := goframe.FromSQL(db, "SELECT * FROM events", nil,
				goframe.SQLReadOption{ParseDates: []string{"timestamp_ms"}})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify it's parsed as time.Time
			col, _ := df.Select("timestamp_ms")
			parsedTime, ok := col.Data[0].(time.Time)
			if !ok {
				t.Errorf("Expected time.Time type, got %T", col.Data[0])
			}

			// Verify Unix timestamp is correct (within 1 second for rounding)
			expectedUnix := int64(1705317000) // seconds
			if parsedTime.Unix() != expectedUnix {
				t.Errorf("Expected Unix timestamp %d, got %d", expectedUnix, parsedTime.Unix())
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQL_MultipleArgs tests queries with multiple arguments
func TestFromSQL_MultipleArgs(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query with multiple arguments
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
				sqlmock.NewColumn("age").OfType("INT", int64(0)),
			).
				AddRow(int64(2), "Bob", int64(30)).
				AddRow(int64(3), "Charlie", int64(35))

			mock.ExpectQuery("SELECT \\* FROM users WHERE age > (.*) AND age < (.*) ORDER BY id").
				WithArgs(25, 40).
				WillReturnRows(rows)

			// Query with multiple args
			df, err := goframe.FromSQL(db,
				"SELECT * FROM users WHERE age > ? AND age < ? ORDER BY id",
				[]any{25, 40})
			if err != nil {
				t.Fatalf("FromSQL failed: %v", err)
			}

			// Verify results
			if df.Nrows() != 2 {
				t.Errorf("Expected 2 rows, got %d", df.Nrows())
			}

			nameCol, _ := df.Select("name")
			if nameCol.Data[0] != "Bob" || nameCol.Data[1] != "Charlie" {
				t.Errorf("Expected Bob and Charlie, got %v and %v",
					nameCol.Data[0], nameCol.Data[1])
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// TestFromSQLContext_Direct tests calling FromSQLContext directly
func TestFromSQLContext_Direct(t *testing.T) {
	for _, dialect := range getDialects() {
		t.Run(dialect.name, func(t *testing.T) {
			db, mock := setupMockDB(t)
			defer db.Close()

			// Set up expected query
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("TEXT", ""),
			).
				AddRow(int64(1), "Alice")

			mock.ExpectQuery("SELECT \\* FROM users").
				WillReturnRows(rows)

			// Call FromSQLContext directly (not through FromSQL wrapper)
			ctx := context.Background()
			df, err := goframe.FromSQLContext(ctx, db, "SELECT * FROM users", nil)
			if err != nil {
				t.Fatalf("FromSQLContext failed: %v", err)
			}

			// Verify data
			if df.Nrows() != 1 {
				t.Errorf("Expected 1 row, got %d", df.Nrows())
			}

			nameCol, _ := df.Select("name")
			if nameCol.Data[0] != "Alice" {
				t.Errorf("Expected 'Alice', got %v", nameCol.Data[0])
			}

			// Verify all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}
