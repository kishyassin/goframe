package goframe_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/kishyassin/goframe/dataframe"
)

// setupTestDB creates an in-memory SQLite database for testing
func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	return db
}

// TestToSQL_CreateAndInsert tests basic write functionality
func TestToSQL_CreateAndInsert(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test DataFrame
	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("age", []int{25, 30, 35})))
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("salary", []float64{50000.0, 60000.0, 75000.0})))

	// Write to database
	err := df.ToSQL(db, "users")
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	// Verify table was created and data inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	// Verify data
	rows, err := db.Query("SELECT name, age, salary FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name   string
		age    int
		salary float64
	}{
		{"Alice", 25, 50000.0},
		{"Bob", 30, 60000.0},
		{"Charlie", 35, 75000.0},
	}

	i := 0
	for rows.Next() {
		var name string
		var age int
		var salary float64
		if err := rows.Scan(&name, &age, &salary); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if name != expected[i].name || age != expected[i].age || salary != expected[i].salary {
			t.Errorf("Row %d: expected (%s, %d, %.2f), got (%s, %d, %.2f)",
				i, expected[i].name, expected[i].age, expected[i].salary, name, age, salary)
		}
		i++
	}
}

// TestToSQL_IfExistsFail tests that ToSQL fails if table already exists
func TestToSQL_IfExistsFail(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test DataFrame
	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

	// First write should succeed
	err := df.ToSQL(db, "test_table")
	if err != nil {
		t.Fatalf("First ToSQL failed: %v", err)
	}

	// Second write should fail with IfExists="fail" (default)
	err = df.ToSQL(db, "test_table")
	if err == nil {
		t.Error("Expected error when writing to existing table with IfExists='fail', got nil")
	}
}

// TestToSQL_IfExistsReplace tests DROP and CREATE behavior
func TestToSQL_IfExistsReplace(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create initial DataFrame
	df1 := dataframe.NewDataFrame()
	df1.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2, 3})))
	df1.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{"A", "B", "C"})))

	// Write initial data
	err := df1.ToSQL(db, "test_table")
	if err != nil {
		t.Fatalf("Initial ToSQL failed: %v", err)
	}

	// Create replacement DataFrame with different schema
	df2 := dataframe.NewDataFrame()
	df2.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{10, 20})))
	df2.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("value", []float64{1.5, 2.5})))

	// Replace the table
	err = df2.ToSQL(db, "test_table", dataframe.SQLWriteOption{IfExists: "replace"})
	if err != nil {
		t.Fatalf("ToSQL with IfExists='replace' failed: %v", err)
	}

	// Verify the new data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows after replace, got %d", count)
	}

	// Verify new column exists
	var value float64
	err = db.QueryRow("SELECT value FROM test_table WHERE id=10").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query new column: %v", err)
	}
	if value != 1.5 {
		t.Errorf("Expected value 1.5, got %.2f", value)
	}
}

// TestToSQL_IfExistsAppend tests appending to existing table
func TestToSQL_IfExistsAppend(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create initial DataFrame
	df1 := dataframe.NewDataFrame()
	df1.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))
	df1.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{"Alice", "Bob"})))

	// Write initial data
	err := df1.ToSQL(db, "test_table")
	if err != nil {
		t.Fatalf("Initial ToSQL failed: %v", err)
	}

	// Create additional DataFrame
	df2 := dataframe.NewDataFrame()
	df2.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{3, 4})))
	df2.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{"Charlie", "Dave"})))

	// Append to the table
	err = df2.ToSQL(db, "test_table", dataframe.SQLWriteOption{IfExists: "append"})
	if err != nil {
		t.Fatalf("ToSQL with IfExists='append' failed: %v", err)
	}

	// Verify total row count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4 rows after append, got %d", count)
	}

	// Verify all data is present
	var names []string
	rows, err := db.Query("SELECT name FROM test_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query names: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Failed to scan name: %v", err)
		}
		names = append(names, name)
	}

	expected := []string{"Alice", "Bob", "Charlie", "Dave"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("Row %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// TestToSQL_EmptyDataFrame tests writing an empty DataFrame
func TestToSQL_EmptyDataFrame(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create empty DataFrame with schema
	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{})))
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("name", []string{})))

	// Write to database
	err := df.ToSQL(db, "empty_table")
	if err != nil {
		t.Fatalf("ToSQL with empty DataFrame failed: %v", err)
	}

	// Verify table was created
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM empty_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query empty table: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows in empty table, got %d", count)
	}
}

// TestToSQL_WithNilValues tests handling of nil values
func TestToSQL_WithNilValues(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create DataFrame with nil values
	df := dataframe.NewDataFrame()
	nameData := []any{"Alice", nil, "Charlie"}
	ageData := []any{25, 30, nil}
	df.AddColumn(dataframe.NewColumn("name", nameData))
	df.AddColumn(dataframe.NewColumn("age", ageData))

	// Write to database
	err := df.ToSQL(db, "users_with_nulls")
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	// Verify nil values were stored as NULL
	rows, err := db.Query("SELECT name, age FROM users_with_nulls ORDER BY ROWID")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name  sql.NullString
		age   sql.NullInt64
	}{
		{sql.NullString{String: "Alice", Valid: true}, sql.NullInt64{Int64: 25, Valid: true}},
		{sql.NullString{Valid: false}, sql.NullInt64{Int64: 30, Valid: true}},
		{sql.NullString{String: "Charlie", Valid: true}, sql.NullInt64{Valid: false}},
	}

	i := 0
	for rows.Next() {
		var name sql.NullString
		var age sql.NullInt64
		if err := rows.Scan(&name, &age); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if name.Valid != expected[i].name.Valid || (name.Valid && name.String != expected[i].name.String) {
			t.Errorf("Row %d name: expected %+v, got %+v", i, expected[i].name, name)
		}
		if age.Valid != expected[i].age.Valid || (age.Valid && age.Int64 != expected[i].age.Int64) {
			t.Errorf("Row %d age: expected %+v, got %+v", i, expected[i].age, age)
		}
		i++
	}
}

// TestToSQL_AllTypes tests all supported data types
func TestToSQL_AllTypes(t *testing.T) {
	db := setupTestDB(t)
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

	// Write to database
	err := df.ToSQL(db, "all_types")
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	// Verify data
	var (
		colInt     int
		colInt64   int64
		colFloat64 float64
		colString  string
		colBool    bool
		colTime    time.Time
	)

	err = db.QueryRow("SELECT col_int, col_int64, col_float64, col_string, col_bool, col_time FROM all_types WHERE col_int=1").
		Scan(&colInt, &colInt64, &colFloat64, &colString, &colBool, &colTime)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}

	if colInt != 1 {
		t.Errorf("Expected col_int=1, got %d", colInt)
	}
	if colInt64 != 100 {
		t.Errorf("Expected col_int64=100, got %d", colInt64)
	}
	if colFloat64 != 1.5 {
		t.Errorf("Expected col_float64=1.5, got %.2f", colFloat64)
	}
	if colString != "hello" {
		t.Errorf("Expected col_string='hello', got %s", colString)
	}
	if !colBool {
		t.Error("Expected col_bool=true, got false")
	}
	if !colTime.Equal(now) {
		t.Errorf("Expected col_time=%v, got %v", now, colTime)
	}
}

// TestToSQL_CustomTypeMap tests custom type mapping
func TestToSQL_CustomTypeMap(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create DataFrame
	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("email", []string{"alice@example.com", "bob@example.com"})))

	// Write with custom type map
	err := df.ToSQL(db, "users", dataframe.SQLWriteOption{
		TypeMap: map[string]string{
			"id":    "INTEGER PRIMARY KEY",
			"email": "VARCHAR(255)",
		},
	})
	if err != nil {
		t.Fatalf("ToSQL with TypeMap failed: %v", err)
	}

	// Verify table schema (SQLite)
	rows, err := db.Query("PRAGMA table_info(users)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	columnTypes := make(map[string]string)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, dfltValue, pk any
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("Failed to scan table info: %v", err)
		}
		columnTypes[name] = colType
	}

	// Verify id is INTEGER (PRIMARY KEY part may not show in type)
	if columnTypes["id"] != "INTEGER" && columnTypes["id"] != "INTEGER PRIMARY KEY" {
		t.Errorf("Expected id type to be INTEGER or INTEGER PRIMARY KEY, got %s", columnTypes["id"])
	}

	// Verify email is VARCHAR(255)
	if columnTypes["email"] != "VARCHAR(255)" {
		t.Errorf("Expected email type to be VARCHAR(255), got %s", columnTypes["email"])
	}
}

// TestToSQL_BatchInsert tests batch insertion with large dataset
func TestToSQL_BatchInsert(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create large DataFrame (5000 rows)
	const numRows = 5000
	ids := make([]int, numRows)
	values := make([]float64, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = i + 1
		values[i] = float64(i) * 1.5
	}

	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", ids)))
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("value", values)))

	// Write with custom batch size
	err := df.ToSQL(db, "large_table", dataframe.SQLWriteOption{
		BatchSize: 1000,
	})
	if err != nil {
		t.Fatalf("ToSQL with large dataset failed: %v", err)
	}

	// Verify row count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM large_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}

	// Spot check some values
	var value float64
	err = db.QueryRow("SELECT value FROM large_table WHERE id=1000").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query value: %v", err)
	}
	expected := float64(999) * 1.5
	if value != expected {
		t.Errorf("Expected value %.2f, got %.2f", expected, value)
	}
}

// TestToSQLContext tests context support
func TestToSQLContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create test DataFrame
	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

	// Write with context
	ctx := context.Background()
	err := df.ToSQLContext(ctx, db, "test_table")
	if err != nil {
		t.Fatalf("ToSQLContext failed: %v", err)
	}

	// Verify data was written
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestToSQLTx tests transaction support
func TestToSQLTx(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Create test DataFrame
	df1 := dataframe.NewDataFrame()
	df1.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

	df2 := dataframe.NewDataFrame()
	df2.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{3, 4})))

	// Write both DataFrames in same transaction
	err = df1.ToSQLTx(tx, "table1")
	if err != nil {
		t.Fatalf("First ToSQLTx failed: %v", err)
	}

	err = df2.ToSQLTx(tx, "table2")
	if err != nil {
		t.Fatalf("Second ToSQLTx failed: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify both tables were created
	var count1, count2 int
	if err := db.QueryRow("SELECT COUNT(*) FROM table1").Scan(&count1); err != nil {
		t.Fatalf("Failed to count table1: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM table2").Scan(&count2); err != nil {
		t.Fatalf("Failed to count table2: %v", err)
	}

	if count1 != 2 {
		t.Errorf("Expected 2 rows in table1, got %d", count1)
	}
	if count2 != 2 {
		t.Errorf("Expected 2 rows in table2, got %d", count2)
	}
}

// TestToSQLTx_Rollback tests transaction rollback
func TestToSQLTx_Rollback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Create test DataFrame
	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

	// Write data
	err = df.ToSQLTx(tx, "test_table")
	if err != nil {
		t.Fatalf("ToSQLTx failed: %v", err)
	}

	// Rollback instead of commit
	tx.Rollback()

	// Verify table doesn't exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err == nil {
		t.Error("Expected error querying rolled-back table, got nil")
	}
}

// TestToSQL_InvalidOptions tests error handling for invalid options
func TestToSQL_InvalidOptions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1})))

	// Test invalid IfExists
	err := df.ToSQL(db, "test", dataframe.SQLWriteOption{IfExists: "invalid"})
	if err == nil {
		t.Error("Expected error for invalid IfExists option, got nil")
	}

	// Test invalid BatchSize (negative)
	err = df.ToSQL(db, "test", dataframe.SQLWriteOption{BatchSize: -1})
	if err == nil {
		t.Error("Expected error for negative BatchSize, got nil")
	}

	// Note: We can't test BatchSize: 0 because Go can't distinguish between
	// "not set" (zero value) and "explicitly set to 0" without using pointers

	// Test invalid Dialect
	err = df.ToSQL(db, "test", dataframe.SQLWriteOption{Dialect: "invalid"})
	if err == nil {
		t.Error("Expected error for invalid Dialect, got nil")
	}
}

// TestToSQL_ExplicitDialect tests specifying dialect explicitly
func TestToSQL_ExplicitDialect(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	df := dataframe.NewDataFrame()
	df.AddColumn(dataframe.ConvertToAnyColumn(dataframe.NewColumn("id", []int{1, 2})))

	// Write with explicit SQLite dialect
	err := df.ToSQL(db, "test_table", dataframe.SQLWriteOption{
		Dialect: "sqlite",
	})
	if err != nil {
		t.Fatalf("ToSQL with explicit dialect failed: %v", err)
	}

	// Verify data was written
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}
