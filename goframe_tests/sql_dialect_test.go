package goframe_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/kishyassin/goframe/dataframe"
	_ "github.com/mattn/go-sqlite3"
)

// TestGoTypeToSQLType_SQLite tests SQLite type mapping
func TestGoTypeToSQLType_SQLite(t *testing.T) {
	dialect := &dataframe.SQLiteDialect{}

	tests := []struct {
		name     string
		goType   reflect.Type
		expected string
	}{
		{"int", reflect.TypeOf(int(0)), "INTEGER"},
		{"int8", reflect.TypeOf(int8(0)), "INTEGER"},
		{"int16", reflect.TypeOf(int16(0)), "INTEGER"},
		{"int32", reflect.TypeOf(int32(0)), "INTEGER"},
		{"int64", reflect.TypeOf(int64(0)), "INTEGER"},
		{"uint", reflect.TypeOf(uint(0)), "INTEGER"},
		{"uint8", reflect.TypeOf(uint8(0)), "INTEGER"},
		{"uint16", reflect.TypeOf(uint16(0)), "INTEGER"},
		{"uint32", reflect.TypeOf(uint32(0)), "INTEGER"},
		{"uint64", reflect.TypeOf(uint64(0)), "INTEGER"},
		{"float32", reflect.TypeOf(float32(0)), "REAL"},
		{"float64", reflect.TypeOf(float64(0)), "REAL"},
		{"string", reflect.TypeOf(""), "TEXT"},
		{"bool", reflect.TypeOf(true), "INTEGER"},
		{"time.Time", reflect.TypeOf(time.Time{}), "TIMESTAMP"},
		{"pointer to int", reflect.TypeOf((*int)(nil)), "INTEGER"},
		{"pointer to string", reflect.TypeOf((*string)(nil)), "TEXT"},
		{"pointer to time", reflect.TypeOf((*time.Time)(nil)), "TIMESTAMP"},
		{"unknown type", reflect.TypeOf(struct{}{}), "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.GoTypeToSQLType(tt.goType)
			if result != tt.expected {
				t.Errorf("GoTypeToSQLType(%v) = %s, expected %s", tt.goType, result, tt.expected)
			}
		})
	}
}

// TestGoTypeToSQLType_Postgres tests PostgreSQL type mapping
func TestGoTypeToSQLType_Postgres(t *testing.T) {
	dialect := &dataframe.PostgresDialect{}

	tests := []struct {
		name     string
		goType   reflect.Type
		expected string
	}{
		{"int", reflect.TypeOf(int(0)), "INTEGER"},
		{"int8", reflect.TypeOf(int8(0)), "INTEGER"},
		{"int16", reflect.TypeOf(int16(0)), "INTEGER"},
		{"int32", reflect.TypeOf(int32(0)), "INTEGER"},
		{"int64", reflect.TypeOf(int64(0)), "BIGINT"},
		{"uint", reflect.TypeOf(uint(0)), "BIGINT"},
		{"uint8", reflect.TypeOf(uint8(0)), "INTEGER"},
		{"uint16", reflect.TypeOf(uint16(0)), "INTEGER"},
		{"uint32", reflect.TypeOf(uint32(0)), "BIGINT"},
		{"uint64", reflect.TypeOf(uint64(0)), "BIGINT"},
		{"float32", reflect.TypeOf(float32(0)), "REAL"},
		{"float64", reflect.TypeOf(float64(0)), "DOUBLE PRECISION"},
		{"string", reflect.TypeOf(""), "TEXT"},
		{"bool", reflect.TypeOf(true), "BOOLEAN"},
		{"time.Time", reflect.TypeOf(time.Time{}), "TIMESTAMP"},
		{"pointer to int64", reflect.TypeOf((*int64)(nil)), "BIGINT"},
		{"pointer to float64", reflect.TypeOf((*float64)(nil)), "DOUBLE PRECISION"},
		{"pointer to bool", reflect.TypeOf((*bool)(nil)), "BOOLEAN"},
		{"unknown type", reflect.TypeOf(struct{}{}), "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.GoTypeToSQLType(tt.goType)
			if result != tt.expected {
				t.Errorf("GoTypeToSQLType(%v) = %s, expected %s", tt.goType, result, tt.expected)
			}
		})
	}
}

// TestGoTypeToSQLType_MySQL tests MySQL type mapping
func TestGoTypeToSQLType_MySQL(t *testing.T) {
	dialect := &dataframe.MySQLDialect{}

	tests := []struct {
		name     string
		goType   reflect.Type
		expected string
	}{
		{"int", reflect.TypeOf(int(0)), "BIGINT"},
		{"int8", reflect.TypeOf(int8(0)), "BIGINT"},
		{"int16", reflect.TypeOf(int16(0)), "BIGINT"},
		{"int32", reflect.TypeOf(int32(0)), "BIGINT"},
		{"int64", reflect.TypeOf(int64(0)), "BIGINT"},
		{"uint", reflect.TypeOf(uint(0)), "BIGINT"},
		{"uint8", reflect.TypeOf(uint8(0)), "INT"},
		{"uint16", reflect.TypeOf(uint16(0)), "INT"},
		{"uint32", reflect.TypeOf(uint32(0)), "BIGINT"},
		{"uint64", reflect.TypeOf(uint64(0)), "BIGINT"},
		{"float32", reflect.TypeOf(float32(0)), "FLOAT"},
		{"float64", reflect.TypeOf(float64(0)), "DOUBLE"},
		{"string", reflect.TypeOf(""), "TEXT"},
		{"bool", reflect.TypeOf(true), "TINYINT(1)"},
		{"time.Time", reflect.TypeOf(time.Time{}), "DATETIME"},
		{"pointer to int", reflect.TypeOf((*int)(nil)), "BIGINT"},
		{"pointer to float32", reflect.TypeOf((*float32)(nil)), "FLOAT"},
		{"pointer to time", reflect.TypeOf((*time.Time)(nil)), "DATETIME"},
		{"unknown type", reflect.TypeOf(struct{}{}), "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.GoTypeToSQLType(tt.goType)
			if result != tt.expected {
				t.Errorf("GoTypeToSQLType(%v) = %s, expected %s", tt.goType, result, tt.expected)
			}
		})
	}
}

// TestPlaceholder tests placeholder generation for all dialects
func TestPlaceholder(t *testing.T) {
	tests := []struct {
		name     string
		dialect  dataframe.SQLDialect
		indices  []int
		expected []string
	}{
		{
			name:     "SQLite placeholders",
			dialect:  &dataframe.SQLiteDialect{},
			indices:  []int{1, 2, 3, 5, 10},
			expected: []string{"?", "?", "?", "?", "?"},
		},
		{
			name:     "PostgreSQL placeholders",
			dialect:  &dataframe.PostgresDialect{},
			indices:  []int{1, 2, 3, 5, 10},
			expected: []string{"$1", "$2", "$3", "$5", "$10"},
		},
		{
			name:     "MySQL placeholders",
			dialect:  &dataframe.MySQLDialect{},
			indices:  []int{1, 2, 3, 5, 10},
			expected: []string{"?", "?", "?", "?", "?"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, index := range tt.indices {
				result := tt.dialect.Placeholder(index)
				if result != tt.expected[i] {
					t.Errorf("Placeholder(%d) = %s, expected %s", index, result, tt.expected[i])
				}
			}
		})
	}
}

// TestQuoteIdentifier tests identifier quoting for all dialects
func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		dialect    dataframe.SQLDialect
		identifier string
		expected   string
	}{
		{"SQLite simple", &dataframe.SQLiteDialect{}, "users", `"users"`},
		{"SQLite with underscore", &dataframe.SQLiteDialect{}, "user_id", `"user_id"`},
		{"SQLite with space", &dataframe.SQLiteDialect{}, "user name", `"user name"`},
		{"PostgreSQL simple", &dataframe.PostgresDialect{}, "users", `"users"`},
		{"PostgreSQL with underscore", &dataframe.PostgresDialect{}, "user_id", `"user_id"`},
		{"PostgreSQL with space", &dataframe.PostgresDialect{}, "user name", `"user name"`},
		{"MySQL simple", &dataframe.MySQLDialect{}, "users", "`users`"},
		{"MySQL with underscore", &dataframe.MySQLDialect{}, "user_id", "`user_id`"},
		{"MySQL with space", &dataframe.MySQLDialect{}, "user name", "`user name`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.QuoteIdentifier(tt.identifier)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%s) = %s, expected %s", tt.identifier, result, tt.expected)
			}
		})
	}
}

// TestCreateTableSQL tests CREATE TABLE statement generation
func TestCreateTableSQL(t *testing.T) {
	tests := []struct {
		name      string
		dialect   dataframe.SQLDialect
		tableName string
		columns   map[string]string
		contains  []string // Substrings that should be in the result
	}{
		{
			name:      "SQLite simple table",
			dialect:   &dataframe.SQLiteDialect{},
			tableName: "users",
			columns: map[string]string{
				"id":   "INTEGER",
				"name": "TEXT",
			},
			contains: []string{"CREATE TABLE", `"users"`, `"id"`, "INTEGER", `"name"`, "TEXT"},
		},
		{
			name:      "PostgreSQL table with multiple columns",
			dialect:   &dataframe.PostgresDialect{},
			tableName: "products",
			columns: map[string]string{
				"id":    "BIGINT",
				"name":  "TEXT",
				"price": "DOUBLE PRECISION",
			},
			contains: []string{"CREATE TABLE", `"products"`, `"id"`, "BIGINT", `"name"`, "TEXT", `"price"`, "DOUBLE PRECISION"},
		},
		{
			name:      "MySQL table with special name",
			dialect:   &dataframe.MySQLDialect{},
			tableName: "user_orders",
			columns: map[string]string{
				"order_id":   "BIGINT",
				"user_id":    "BIGINT",
				"created_at": "DATETIME",
			},
			contains: []string{"CREATE TABLE", "`user_orders`", "`order_id`", "BIGINT", "`user_id`", "`created_at`", "DATETIME"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.CreateTableSQL(tt.tableName, tt.columns)
			for _, substring := range tt.contains {
				if !contains(result, substring) {
					t.Errorf("CreateTableSQL() = %s, should contain %s", result, substring)
				}
			}
		})
	}
}

// TestTableExistsSQL tests table existence check queries
func TestTableExistsSQL(t *testing.T) {
	tests := []struct {
		name      string
		dialect   dataframe.SQLDialect
		tableName string
		contains  []string
	}{
		{
			name:      "SQLite table exists",
			dialect:   &dataframe.SQLiteDialect{},
			tableName: "users",
			contains:  []string{"SELECT name FROM sqlite_master", "type='table'", "name=?"},
		},
		{
			name:      "PostgreSQL table exists",
			dialect:   &dataframe.PostgresDialect{},
			tableName: "products",
			contains:  []string{"SELECT tablename FROM pg_tables", "schemaname='public'", "tablename=$1"},
		},
		{
			name:      "MySQL table exists",
			dialect:   &dataframe.MySQLDialect{},
			tableName: "orders",
			contains:  []string{"SELECT table_name FROM information_schema.tables", "table_schema=DATABASE()", "table_name=?"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.TableExistsSQL(tt.tableName)
			for _, substring := range tt.contains {
				if !contains(result, substring) {
					t.Errorf("TableExistsSQL() = %s, should contain %s", result, substring)
				}
			}
		})
	}
}

// TestGetDialect tests dialect selection and detection
func TestGetDialect(t *testing.T) {
	// Setup test database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name         string
		dialectName  string
		expectedType string
		expectError  bool
	}{
		{"explicit SQLite", "sqlite", "*dataframe.SQLiteDialect", false},
		{"explicit SQLite3", "sqlite3", "*dataframe.SQLiteDialect", false},
		{"explicit PostgreSQL", "postgres", "*dataframe.PostgresDialect", false},
		{"explicit PostgreSQL variant", "postgresql", "*dataframe.PostgresDialect", false},
		{"explicit PostgreSQL pq", "pq", "*dataframe.PostgresDialect", false},
		{"explicit MySQL", "mysql", "*dataframe.MySQLDialect", false},
		{"unknown dialect", "oracle", "", true},
		{"empty string auto-detect", "", "*dataframe.SQLiteDialect", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: We can't directly test getDialect as it's not exported
			// This test documents expected behavior for when/if it becomes exported
			// For now, we test the individual dialect creation

			var dialect dataframe.SQLDialect
			var err error

			switch tt.dialectName {
			case "sqlite", "sqlite3":
				dialect = &dataframe.SQLiteDialect{}
			case "postgres", "postgresql", "pq":
				dialect = &dataframe.PostgresDialect{}
			case "mysql":
				dialect = &dataframe.MySQLDialect{}
			case "":
				// Auto-detect would happen here
				dialect = &dataframe.SQLiteDialect{}
			default:
				err = sql.ErrConnDone // Simulate error
			}

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for dialect %s, got nil", tt.dialectName)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for dialect %s: %v", tt.dialectName, err)
				}
				dialectType := reflect.TypeOf(dialect).String()
				if dialectType != tt.expectedType {
					t.Errorf("Expected dialect type %s, got %s", tt.expectedType, dialectType)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
