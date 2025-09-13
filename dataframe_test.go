package goframe

import (
	"strings"
	"testing"
)

func TestSeriesBasic(t *testing.T) {
	data := []interface{}{1.0, 2.0, 3.0, 4.0}
	series := NewSeries("test", data)

	if series.Name != "test" {
		t.Errorf("Expected name 'test', got '%s'", series.Name)
	}

	if series.Len() != 4 {
		t.Errorf("Expected length 4, got %d", series.Len())
	}

	if series.At(0) != 1.0 {
		t.Errorf("Expected 1.0 at index 0, got %v", series.At(0))
	}
}

func TestSeriesStats(t *testing.T) {
	data := []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}
	series := NewSeries("numbers", data)

	// Test Mean
	mean, err := series.Mean()
	if err != nil {
		t.Errorf("Error calculating mean: %v", err)
	}
	if mean != 3.0 {
		t.Errorf("Expected mean 3.0, got %f", mean)
	}

	// Test Sum
	sum, err := series.Sum()
	if err != nil {
		t.Errorf("Error calculating sum: %v", err)
	}
	if sum != 15.0 {
		t.Errorf("Expected sum 15.0, got %f", sum)
	}

	// Test Min
	min, err := series.Min()
	if err != nil {
		t.Errorf("Error calculating min: %v", err)
	}
	if min != 1.0 {
		t.Errorf("Expected min 1.0, got %f", min)
	}

	// Test Max
	max, err := series.Max()
	if err != nil {
		t.Errorf("Error calculating max: %v", err)
	}
	if max != 5.0 {
		t.Errorf("Expected max 5.0, got %f", max)
	}
}

func TestDataFrameBasic(t *testing.T) {
	df := NewDataFrame()

	// Test empty DataFrame
	if df.Nrows() != 0 {
		t.Errorf("Expected 0 rows, got %d", df.Nrows())
	}
	if df.Ncols() != 0 {
		t.Errorf("Expected 0 columns, got %d", df.Ncols())
	}

	// Add a column
	data := []interface{}{1.0, 2.0, 3.0}
	series := NewSeries("col1", data)
	err := df.AddColumn(series)
	if err != nil {
		t.Errorf("Error adding column: %v", err)
	}

	if df.Nrows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Nrows())
	}
	if df.Ncols() != 1 {
		t.Errorf("Expected 1 column, got %d", df.Ncols())
	}
}

func TestDataFrameSelect(t *testing.T) {
	df := NewDataFrame()

	// Add columns
	col1 := NewSeries("age", []interface{}{25.0, 30.0, 35.0})
	col2 := NewSeries("name", []interface{}{"Alice", "Bob", "Charlie"})

	df.AddColumn(col1)
	df.AddColumn(col2)

	// Test selecting existing column
	selected, err := df.Select("age")
	if err != nil {
		t.Errorf("Error selecting column: %v", err)
	}
	if selected.Name != "age" {
		t.Errorf("Expected column name 'age', got '%s'", selected.Name)
	}

	// Test selecting non-existing column
	_, err = df.Select("nonexistent")
	if err == nil {
		t.Errorf("Expected error when selecting non-existing column")
	}
}

func TestDataFrameFilter(t *testing.T) {
	df := NewDataFrame()

	// Add columns
	ages := NewSeries("age", []interface{}{25.0, 30.0, 35.0, 40.0})
	names := NewSeries("name", []interface{}{"Alice", "Bob", "Charlie", "David"})

	df.AddColumn(ages)
	df.AddColumn(names)

	// Filter rows where age > 30
	filtered := df.Filter(func(row []interface{}) bool {
		age, ok := row[0].(float64)
		return ok && age > 30
	})

	if filtered.Nrows() != 2 {
		t.Errorf("Expected 2 rows after filtering, got %d", filtered.Nrows())
	}

	// Check first filtered row
	ageCol, _ := filtered.Select("age")
	if ageCol.At(0) != 35.0 {
		t.Errorf("Expected first filtered age to be 35.0, got %v", ageCol.At(0))
	}
}

func TestFromCSV(t *testing.T) {
	// Create a test CSV string
	csvData := `name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000`

	reader := strings.NewReader(csvData)
	df, err := FromCSVReader(reader)
	if err != nil {
		t.Errorf("Error reading CSV: %v", err)
	}

	if df.Nrows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Nrows())
	}

	if df.Ncols() != 3 {
		t.Errorf("Expected 3 columns, got %d", df.Ncols())
	}

	// Test column names
	names := df.ColumnNames()
	expectedNames := []string{"name", "age", "salary"}
	for i, expected := range expectedNames {
		if names[i] != expected {
			t.Errorf("Expected column name '%s', got '%s'", expected, names[i])
		}
	}

	// Test data
	nameCol, _ := df.Select("name")
	if nameCol.At(0) != "Alice" {
		t.Errorf("Expected first name to be 'Alice', got %v", nameCol.At(0))
	}

	ageCol, _ := df.Select("age")
	if ageCol.At(0) != 25.0 {
		t.Errorf("Expected first age to be 25.0, got %v", ageCol.At(0))
	}
}

func TestToCSV(t *testing.T) {
	df := NewDataFrame()

	// Add test data
	names := NewSeries("name", []interface{}{"Alice", "Bob"})
	ages := NewSeries("age", []interface{}{25.0, 30.0})

	df.AddColumn(names)
	df.AddColumn(ages)

	// Export to CSV string
	var buffer strings.Builder
	err := df.ToCSVWriter(&buffer)
	if err != nil {
		t.Errorf("Error writing CSV: %v", err)
	}

	result := buffer.String()
	expected := "name,age\nAlice,25\nBob,30\n"

	if result != expected {
		t.Errorf("Expected CSV:\n%s\nGot:\n%s", expected, result)
	}
}
