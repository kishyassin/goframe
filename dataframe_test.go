package goframe

import (
	"reflect"
	"strings"
	"testing"
)

func TestColumnBasic(t *testing.T) {
	// Test creation of Column[int]
	intCol := NewColumn("intColumn", []int{1, 2, 3, 4})
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
	stringCol := NewColumn("stringColumn", []string{"a", "b", "c"})
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
	df := NewDataFrame()

	// Test adding a column
	intCol := &Column[any]{
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

	df, err := FromCSVReader(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Validate column names
	expectedColumns := []string{"name", "age", "salary"}
	if !reflect.DeepEqual(df.ColumnNames(), expectedColumns) {
		t.Errorf("Expected columns %v, got %v", expectedColumns, df.ColumnNames())
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
