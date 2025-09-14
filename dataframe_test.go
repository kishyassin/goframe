package goframe_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/kishyassin/goframe"
)

func TestColumnBasic(t *testing.T) {
	// Test creation of Column[int]
	intCol := goframe.NewColumn("intColumn", []int{1, 2, 3, 4})
	if intCol.Name != "intColumn" {
		t.Errorf("Expected column name 'intColumn', got '%s'", intCol.Name)
	}
	if intCol.Len() != 4 {
		t.Errorf("Expected length 4, got %d", intCol.Len())
	}

	// Test At method
	val, err := intCol.At(2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 3 {
		t.Errorf("Expected value 3, got %v", val)
	}

	// Test out-of-bounds access
	_, err = intCol.At(10)
	if err == nil {
		t.Errorf("Expected error for out-of-bounds access, got nil")
	}

	// Test creation of Column[string]
	stringCol := goframe.NewColumn("stringColumn", []string{"a", "b", "c"})
	if stringCol.Name != "stringColumn" {
		t.Errorf("Expected column name 'stringColumn', got '%s'", stringCol.Name)
	}
	if stringCol.Len() != 3 {
		t.Errorf("Expected length 3, got %d", stringCol.Len())
	}

	// Test At method for string column
	strVal, err := stringCol.At(1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if strVal != "b" {
		t.Errorf("Expected value 'b', got '%v'", strVal)
	}
}

func TestDataFrameAddDropColumn(t *testing.T) {
	df := goframe.NewDataFrame()

	// Test adding a column
	intCol := &goframe.Column[any]{
		Name: "intColumn",
		Data: []any{1, 2, 3},
	}
	err := df.AddColumn(intCol)
	if err != nil {
		t.Errorf("Unexpected error adding column: %v", err)
	}
	if len(df.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(df.Columns))
	}

	// Test adding a duplicate column
	err = df.AddColumn(intCol)
	if err == nil {
		t.Errorf("Expected error when adding duplicate column, got nil")
	}

	// Test removing a column
	err = df.DropColumn("intColumn")
	if err != nil {
		t.Errorf("Unexpected error removing column: %v", err)
	}
	if len(df.Columns) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(df.Columns))
	}

	// Test removing a non-existent column
	err = df.DropColumn("nonExistentColumn")
	if err == nil {
		t.Errorf("Expected error when removing non-existent column, got nil")
	}
}

func TestFromCSVReader(t *testing.T) {
	csvData := `name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000`
	reader := strings.NewReader(csvData)
	df, err := goframe.FromCSVReader(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Validate column names
	expectedColumns := []string{"name", "age", "salary"}
	sort.Strings(expectedColumns) // Ensure consistent order
	actualColumns := df.ColumnNames()
	t.Logf("Actual column names: %v", actualColumns)
	if !reflect.DeepEqual(actualColumns, expectedColumns) {
		t.Errorf("Expected columns %v, got %v", expectedColumns, actualColumns)
	}

	// Validate column types and data
	nameCol, _ := df.Select("name")
	if nameCol.Data[0] != "Alice" {
		t.Errorf("Expected 'Alice' in name column, got %v", nameCol.Data[0])
	}

	ageCol, _ := df.Select("age")
	if ageCol.Data[0] != 25.0 {
		t.Errorf("Expected 25 in age column, got %v", ageCol.Data[0])
	}

	salaryCol, _ := df.Select("salary")
	if salaryCol.Data[0] != 50000.0 {
		t.Errorf("Expected 50000 in salary column, got %v", salaryCol.Data[0])
	}
}

func TestDataFrameRowOperations(t *testing.T) {
	df := goframe.NewDataFrame()

	// Add initial columns
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("age", []int{25, 30, 35})))

	// Test Row method
	row, err := df.Row(1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if row["name"] != "Bob" || row["age"] != 30 {
		t.Errorf("Expected row {name: Bob, age: 30}, got %v", row)
	}

	// Test Head method
	head := df.Head(2)
	if head.Nrows() != 2 {
		t.Errorf("Expected 2 rows, got %d", head.Nrows())
	}

	// Test Tail method
	tail := df.Tail(2)
	if tail.Nrows() != 2 {
		t.Errorf("Expected 2 rows, got %d", tail.Nrows())
	}

	// Test AppendRow method
	newRow := map[string]any{"name": "Diana", "age": 40}
	err = df.AppendRow(newRow)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if df.Nrows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.Nrows())
	}

	// Test DropRow method
	err = df.DropRow(1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if df.Nrows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Nrows())
	}

	// Test RenameColumn method
	err = df.RenameColumn("name", "full_name")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if _, exists := df.Columns["full_name"]; !exists {
		t.Errorf("Expected column 'full_name' to exist")
	}
}

func TestDataFrameAggregations(t *testing.T) {
	df := goframe.NewDataFrame()

	// Add columns
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("col1", []int{1, 2, 3, 4})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("col2", []float64{1.5, 2.5, 3.5, 4.5})))

	// Test Mean
	means, err := df.Mean()
	if err != nil {
		t.Errorf("Unexpected error calculating mean: %v", err)
	}
	if means["col1"] != 2.5 {
		t.Errorf("Expected mean of col1 to be 2.5, got %v", means["col1"])
	}
	if means["col2"] != 3.0 {
		t.Errorf("Expected mean of col2 to be 3.0, got %v", means["col2"])
	}

	// Test Sum
	sums, err := df.Sum()
	if err != nil {
		t.Errorf("Unexpected error calculating sum: %v", err)
	}
	if sums["col1"] != 10 {
		t.Errorf("Expected sum of col1 to be 10, got %v", sums["col1"])
	}
	if sums["col2"] != 12.0 {
		t.Errorf("Expected sum of col2 to be 12.0, got %v", sums["col2"])
	}

	// Test Min
	mins, err := df.Min()
	if err != nil {
		t.Errorf("Unexpected error calculating min: %v", err)
	}
	if mins["col1"] != 1 {
		t.Errorf("Expected min of col1 to be 1, got %v", mins["col1"])
	}
	if mins["col2"] != 1.5 {
		t.Errorf("Expected min of col2 to be 1.5, got %v", mins["col2"])
	}

	// Test Max
	maxs, err := df.Max()
	if err != nil {
		t.Errorf("Unexpected error calculating max: %v", err)
	}
	if maxs["col1"] != 4 {
		t.Errorf("Expected max of col1 to be 4, got %v", maxs["col1"])
	}
	if maxs["col2"] != 4.5 {
		t.Errorf("Expected max of col2 to be 4.5, got %v", maxs["col2"])
	}
}

func TestDataFrameJoin(t *testing.T) {
	df1 := goframe.NewDataFrame()
	df1.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("id", []int{1, 2, 3})))
	df1.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value1", []string{"A", "B", "C"})))

	df2 := goframe.NewDataFrame()
	df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("id", []int{2, 3, 4})))
	df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value2", []string{"X", "Y", "Z"})))

	// Test inner join
	innerJoin, err := df1.Join(df2, "id", "inner")
	if err != nil {
		t.Errorf("Unexpected error during inner join: %v", err)
	}
	if innerJoin.Nrows() != 2 {
		t.Errorf("Expected 2 rows in inner join, got %d", innerJoin.Nrows())
	}

	// Test left join
	leftJoin, err := df1.Join(df2, "id", "left")
	if err != nil {
		t.Errorf("Unexpected error during left join: %v", err)
	}
	if leftJoin.Nrows() != 3 {
		t.Errorf("Expected 3 rows in left join, got %d", leftJoin.Nrows())
	}

	// Test right join
	rightJoin, err := df1.Join(df2, "id", "right")
	if err != nil {
		t.Errorf("Unexpected error during right join: %v", err)
	}
	if rightJoin.Nrows() != 3 {
		t.Errorf("Expected 3 rows in right join, got %d", rightJoin.Nrows())
	}

	// Test outer join
	outerJoin, err := df1.Join(df2, "id", "outer")
	if err != nil {
		t.Errorf("Unexpected error during outer join: %v", err)
	}
	if outerJoin.Nrows() != 4 {
		t.Errorf("Expected 4 rows in outer join, got %d", outerJoin.Nrows())
	}
}
