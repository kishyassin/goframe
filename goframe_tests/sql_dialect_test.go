package goframe_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kishyassin/goframe/dataframe"
)

	dialect := &dataframe.SQLiteDialect{}

	tests := []struct {
		name     string
		goType   reflect.Type
		expected string
	}{
		// Integer types
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

		// Float types
		{"float32", reflect.TypeOf(float32(0)), "REAL"},
		{"float64", reflect.TypeOf(float64(0)), "REAL"},

		// String type
		{"string", reflect.TypeOf(""), "TEXT"},

		// Bool type
		{"bool", reflect.TypeOf(false), "INTEGER"},

		// Time type
		{"time.Time", reflect.TypeOf(time.Time{}), "TIMESTAMP"},

		// Pointer types
		{"*int", reflect.TypeOf(new(int)), "INTEGER"},
		{"*float64", reflect.TypeOf(new(float64)), "REAL"},
		{"*string", reflect.TypeOf(new(string)), "TEXT"},
		{"*bool", reflect.TypeOf(new(bool)), "INTEGER"},
		{"*time.Time", reflect.TypeOf(new(time.Time)), "TIMESTAMP"},

		// Unknown types
		{"struct", reflect.TypeOf(struct{}{}), "TEXT"},
		{"slice", reflect.TypeOf([]int{}), "TEXT"},
		{"map", reflect.TypeOf(map[string]int{}), "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.GoTypeToSQLType(tt.goType)
			if result != tt.expected {
				t.Errorf("GoTypeToSQLType(%v) = %q, want %q", tt.goType, result, tt.expected)
			}
		})
	}
}

// TestPostgresDialect_GoTypeToSQLType tests PostgreSQL type mapping with table-driven tests
func TestPostgresDialect_GoTypeToSQLType(t *testing.T) {
	dialect := &dataframe.PostgresDialect{}

	tests := []struct {
		name     string
		goType   reflect.Type
		expected string
	}{
		// Integer types
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

		// Float types
		{"float32", reflect.TypeOf(float32(0)), "REAL"},
		{"float64", reflect.TypeOf(float64(0)), "DOUBLE PRECISION"},

		// String type
		{"string", reflect.TypeOf(""), "TEXT"},

		// Bool type
		{"bool", reflect.TypeOf(false), "BOOLEAN"},

		// Time type
		{"time.Time", reflect.TypeOf(time.Time{}), "TIMESTAMP"},

		// Pointer types
		{"*int64", reflect.TypeOf(new(int64)), "BIGINT"},
		{"*float64", reflect.TypeOf(new(float64)), "DOUBLE PRECISION"},
		{"*string", reflect.TypeOf(new(string)), "TEXT"},
		{"*bool", reflect.TypeOf(new(bool)), "BOOLEAN"},
		{"*time.Time", reflect.TypeOf(new(time.Time)), "TIMESTAMP"},

		// Unknown types
		{"struct", reflect.TypeOf(struct{}{}), "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.GoTypeToSQLType(tt.goType)
			if result != tt.expected {
				t.Errorf("GoTypeToSQLType(%v) = %q, want %q", tt.goType, result, tt.expected)
			}
		})
	}
}

// TestMySQLDialect_GoTypeToSQLType tests MySQL type mapping with table-driven tests
func TestMySQLDialect_GoTypeToSQLType(t *testing.T) {
	dialect := &dataframe.MySQLDialect{}

	tests := []struct {
		name     string
		goType   reflect.Type
		expected string
	}{
		// Integer types
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

		// Float types
		{"float32", reflect.TypeOf(float32(0)), "FLOAT"},
		{"float64", reflect.TypeOf(float64(0)), "DOUBLE"},

		// String type
		{"string", reflect.TypeOf(""), "TEXT"},

		// Bool type
		{"bool", reflect.TypeOf(false), "TINYINT(1)"},

		// Time type
		{"time.Time", reflect.TypeOf(time.Time{}), "DATETIME"},

		// Pointer types
		{"*int64", reflect.TypeOf(new(int64)), "BIGINT"},
		{"*float64", reflect.TypeOf(new(float64)), "DOUBLE"},
		{"*string", reflect.TypeOf(new(string)), "TEXT"},
		{"*bool", reflect.TypeOf(new(bool)), "TINYINT(1)"},
		{"*time.Time", reflect.TypeOf(new(time.Time)), "DATETIME"},

		// Unknown types
		{"struct", reflect.TypeOf(struct{}{}), "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.GoTypeToSQLType(tt.goType)
			if result != tt.expected {
				t.Errorf("GoTypeToSQLType(%v) = %q, want %q", tt.goType, result, tt.expected)
			}
		})
	}
}

// TestDialect_Placeholder tests placeholder syntax for all dialects
func TestDialect_Placeholder(t *testing.T) {
	tests := []struct {
		name     string
		dialect  dataframe.SQLDialect
		index    int
		expected string
	}{
		// SQLite
		{"SQLite index 1", &dataframe.SQLiteDialect{}, 1, "?"},
		{"SQLite index 5", &dataframe.SQLiteDialect{}, 5, "?"},
		{"SQLite index 100", &dataframe.SQLiteDialect{}, 100, "?"},

		// PostgreSQL
		{"PostgreSQL index 1", &dataframe.PostgresDialect{}, 1, "$1"},
		{"PostgreSQL index 2", &dataframe.PostgresDialect{}, 2, "$2"},
		{"PostgreSQL index 10", &dataframe.PostgresDialect{}, 10, "$10"},
		{"PostgreSQL index 100", &dataframe.PostgresDialect{}, 100, "$100"},

		// MySQL
		{"MySQL index 1", &dataframe.MySQLDialect{}, 1, "?"},
		{"MySQL index 5", &dataframe.MySQLDialect{}, 5, "?"},
		{"MySQL index 100", &dataframe.MySQLDialect{}, 100, "?"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.Placeholder(tt.index)
			if result != tt.expected {
				t.Errorf("Placeholder(%d) = %q, want %q", tt.index, result, tt.expected)
			}
		})
	}
}

// TestDialect_QuoteIdentifier tests identifier quoting for all dialects
func TestDialect_QuoteIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		dialect    dataframe.SQLDialect
		identifier string
		expected   string
	}{
		// SQLite
		{"SQLite simple", &dataframe.SQLiteDialect{}, "users", `"users"`},
		{"SQLite with space", &dataframe.SQLiteDialect{}, "user name", `"user name"`},
		{"SQLite with underscore", &dataframe.SQLiteDialect{}, "user_id", `"user_id"`},

		// PostgreSQL
		{"PostgreSQL simple", &dataframe.PostgresDialect{}, "users", `"users"`},
		{"PostgreSQL with space", &dataframe.PostgresDialect{}, "user name", `"user name"`},
		{"PostgreSQL with underscore", &dataframe.PostgresDialect{}, "user_id", `"user_id"`},

		// MySQL
		{"MySQL simple", &dataframe.MySQLDialect{}, "users", "`users`"},
		{"MySQL with space", &dataframe.MySQLDialect{}, "user name", "`user name`"},
		{"MySQL with underscore", &dataframe.MySQLDialect{}, "user_id", "`user_id`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.QuoteIdentifier(tt.identifier)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.identifier, result, tt.expected)
			}
		})
	}
}

// TestDialect_CreateTableSQL tests CREATE TABLE SQL generation
func TestDialect_CreateTableSQL(t *testing.T) {
	tests := []struct {
		name      string
		dialect   dataframe.SQLDialect
		tableName string
		columns   map[string]string
		contains  []string // Substrings that should be in the output
	}{
		{
			name:      "SQLite simple table",
			dialect:   &dataframe.SQLiteDialect{},
			tableName: "users",
			columns: map[string]string{
				"id":   "INTEGER",
				"name": "TEXT",
			},
			contains: []string{
				`CREATE TABLE "users"`,
				`"id" INTEGER`,
				`"name" TEXT`,
			},
		},
		{
			name:      "PostgreSQL simple table",
			dialect:   &dataframe.PostgresDialect{},
			tableName: "products",
			columns: map[string]string{
				"id":    "BIGINT",
				"price": "DOUBLE PRECISION",
			},
			contains: []string{
				`CREATE TABLE "products"`,
				`"id" BIGINT`,
				`"price" DOUBLE PRECISION`,
			},
		},
		{
			name:      "MySQL simple table",
			dialect:   &dataframe.MySQLDialect{},
			tableName: "orders",
			columns: map[string]string{
				"id":         "BIGINT",
				"created_at": "DATETIME",
			},
			contains: []string{
				"CREATE TABLE `orders`",
				"`id` BIGINT",
				"`created_at` DATETIME",
			},
		},
		{
			name:      "SQLite table with special chars",
			dialect:   &dataframe.SQLiteDialect{},
			tableName: "user_activity",
			columns: map[string]string{
				"user_id":    "INTEGER",
				"login_time": "TIMESTAMP",
			},
			contains: []string{
				`CREATE TABLE "user_activity"`,
				`"user_id" INTEGER`,
				`"login_time" TIMESTAMP`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.CreateTableSQL(tt.tableName, tt.columns)

			// Check that all expected substrings are present
			for _, substr := range tt.contains {
				if !strings.Contains(result, substr) {
					t.Errorf("CreateTableSQL() output missing expected substring %q\nGot: %s", substr, result)
				}
			}
		})
	}
}

// TestDialect_TableExistsSQL tests table existence check queries
func TestDialect_TableExistsSQL(t *testing.T) {
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
			contains: []string{
				"SELECT name FROM sqlite_master",
				"WHERE type='table'",
				"AND name=?",
			},
		},
		{
			name:      "PostgreSQL table exists",
			dialect:   &dataframe.PostgresDialect{},
			tableName: "products",
			contains: []string{
				"SELECT tablename FROM pg_tables",
				"WHERE schemaname='public'",
				"AND tablename=$1",
			},
		},
		{
			name:      "MySQL table exists",
			dialect:   &dataframe.MySQLDialect{},
			tableName: "orders",
			contains: []string{
				"SELECT table_name FROM information_schema.tables",
				"WHERE table_schema=DATABASE()",
				"AND table_name=?",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dialect.TableExistsSQL()

			// Check that all expected substrings are present
			for _, substr := range tt.contains {
				if !strings.Contains(result, substr) {
					t.Errorf("TableExistsSQL() output missing expected substring %q\nGot: %s", substr, result)
				}
			}
		})
	}
}

