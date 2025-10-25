package goframe_test

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	goframe "github.com/kishyassin/goframe/dataframe"
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
	cases := []struct {
		input       string
		wantError   string
		wantColumns []string
		wantData    map[string][]any
	}{
		{
			input: `product,quantity,discount
					Laptop,3,
					Mouse,10,
					Keyboard,5,`,
			wantColumns: []string{"quantity", "product", "discount"},
			wantData: map[string][]any{
				"quantity": {3.0, 10.0, 5.0},
				"product":  {"Laptop", "Mouse", "Keyboard"},
				"discount": {"", "", ""},
			},
		},
		{
			input: `player,level,points
					Neo,7,1200
					Trinity,12,3400
					Morpheus,20,5600`,
			wantColumns: []string{"level", "player", "points"},
			wantData: map[string][]any{
				"level":  {7.0, 12.0, 20.0},
				"player": {"Neo", "Trinity", "Morpheus"},
				"points": {1200.0, 3400.0, 5600.0},
			},
		},
		{
			input: `city,temp,humidity
					Berlin,18,
					Paris,,55
					,21,60`,
			wantColumns: []string{"city", "humidity", "temp"},
			wantData: map[string][]any{
				"temp":     {18.0, "", 21.0},
				"city":     {"Berlin", "Paris", ""},
				"humidity": {"", 55.0, 60.0},
			},
		},
		{
			input:       ``,
			wantError:   "error reading header",
			wantColumns: nil,
			wantData:    nil,
		},
		{
			input: `name,age
					"Alice,25,
					Bob,30`,
			wantError:   "error reading row",
			wantColumns: nil,
			wantData:    nil,
		},
		{
			input: `name,age,salary
					Alice,25,50000,1
					Bob,30`,
			wantError:   "error reading row",
			wantColumns: nil,
			wantData:    nil,
		},
	}

	for _, tc := range cases {
		reader := strings.NewReader(tc.input)
		df, err := goframe.FromCSVReader(reader)
		if tc.wantError != "" {
			if err == nil {
				t.Fatalf("expected to get an error, got success")
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Unexpected error! The error returned must contain text %q, got %q", tc.wantError, err.Error())
			}

			t.Logf("Success! Expected error %s, got %s ", tc.wantError, err.Error())
			continue
		}

		// Validate column names
		cols := df.ColumnNames()
		t.Logf("Actual column names: %v", cols)

		sort.Strings(tc.wantColumns)

		if !reflect.DeepEqual(cols, tc.wantColumns) {
			t.Errorf("Expected columns %v, got %v", tc.wantColumns, cols)
		}

		// Validate column types and data
		for _, colName := range tc.wantColumns {
			col, _ := df.Select(colName)
			if !reflect.DeepEqual(col.Data, tc.wantData[colName]) {
				t.Errorf("Expected column %v, got %v", tc.wantData[colName], col.Data)
			}
		}
	}
}

func TestDataFrameRowOperations(t *testing.T) {
	df := goframe.NewDataFrame()

	// Add initial columns
	err := df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	err = df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("age", []int{25, 30, 35})))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

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
	err = df.AppendRow(df, newRow)
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
	err := df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("col1", []int{1, 2, 3, 4})))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	err = df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("col2", []float64{1.5, 2.5, 3.5, 4.5})))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	err = df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("badCol", []string{"1.0", "2.0", "3.0", "4.0", "5.0"})))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

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

	// Test Bad Data
	df2 := goframe.NewDataFrame()
	err2 := df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("badCol", []string{"hello", "world"})))
	if err2 != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Test Mean with Bad Data
	means2, err2 := df2.Mean()
	if err2 == nil {
		t.Errorf("Expected an error, got nil instead")
	}
	if means2 != nil {
		t.Errorf("Expected a nil, got %v instead", means2)
	}

	// Test Sum with Bad Data
	sum2, err2 := df2.Sum()
	if err2 == nil {
		t.Errorf("Expected an error, got nil instead")
	}
	if sum2 != nil {
		t.Errorf("Expected a nil, got %v instead", sum2)
	}

	// Test Min with Bad Data
	min, err2 := df2.Min()
	if err2 == nil {
		t.Errorf("Expected an error, got nil instead")
	}
	if min != nil {
		t.Errorf("Expected a nil, got %v instead", min)
	}

	// Test Max with Bad Data
	max, err2 := df2.Max()
	if err2 == nil {
		t.Errorf("Expected an error, got nil instead")
	}
	if max != nil {
		t.Errorf("Expected a nil, got %v instead", max)
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
	innerJoin, err := df1.InnerJoin(df2, "id")
	if err != nil {
		t.Errorf("Unexpected error during inner join: %v", err)
	}
	if innerJoin.Nrows() != 2 {
		t.Errorf("Expected 2 rows in inner join, got %d", innerJoin.Nrows())
	}

	// Test left join
	leftJoin, err := df1.LeftJoin(df2, "id")
	if err != nil {
		t.Errorf("Unexpected error during left join: %v", err)
	}
	if leftJoin.Nrows() != 3 {
		t.Errorf("Expected 3 rows in left join, got %d", leftJoin.Nrows())
	}

	// Test right join
	rightJoin, err := df1.RightJoin(df2, "id")
	if err != nil {
		t.Errorf("Unexpected error during right join: %v", err)
	}
	if rightJoin.Nrows() != 3 {
		t.Errorf("Expected 3 rows in right join, got %d", rightJoin.Nrows())
	}

	// Test outer join
	outerJoin, err := df1.OuterJoin(df2, "id")
	if err != nil {
		t.Errorf("Unexpected error during outer join: %v", err)
	}
	if outerJoin.Nrows() != 4 {
		for _, val := range outerJoin.Columns {
			fmt.Printf("The column name: %v \n", val)
			fmt.Printf("Column values: %v\n", val.Data...)
		}
		t.Errorf("Expected 4 rows in outer join, got %d", outerJoin.Nrows())
	}
}

func TestAdvancedIndexing(t *testing.T) {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("index", []int{1, 2, 3, 4}))) // Add index column
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("id", []int{1, 2, 3, 4})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value", []string{"A", "B", "C", "D"})))

	// Test BooleanIndex
	filtered := df.BooleanIndex(func(row map[string]any) bool {
		return row["id"].(int) > 2
	})
	if filtered.Nrows() != 2 {
		t.Errorf("Expected 2 rows after BooleanIndex, got %d", filtered.Nrows())
	}

	// Test Loc
	locResult, err := df.Loc([]any{1, 3}, []string{"id", "value"})
	if err != nil {
		t.Errorf("Unexpected error in Loc: %v", err)
	}
	if locResult.Nrows() != 2 {
		t.Errorf("Expected 2 rows in Loc result, got %d", locResult.Nrows())
	}

	// Test Iloc
	ilocResult, err := df.Iloc([]int{0, 2}, []int{0, 1})
	if err != nil {
		t.Errorf("Unexpected error in Iloc: %v", err)
	}
	if ilocResult.Nrows() != 2 {
		t.Errorf("Expected 2 rows in Iloc result, got %d", ilocResult.Nrows())
	}
}

func TestVisualization(t *testing.T) {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("x", []float64{1, 2, 3, 4, 5})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("y", []float64{2, 4, 6, 8, 10})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("z", []float64{5, 10, 15, 20, 25})))

	// Test LinePlot
	linePlotFilename := "line_plot_test.png"
	linePlotErr := df.LinePlot("x", "y", linePlotFilename)
	if linePlotErr != nil {
		t.Errorf("LinePlot failed: %v", linePlotErr)
	}

	// Test BarPlot
	barPlotFilename := "bar_plot_test.png"
	barPlotErr := df.BarPlot("z", barPlotFilename)
	if barPlotErr != nil {
		t.Errorf("BarPlot failed: %v", barPlotErr)
	}

	_, err := os.Stat("line_plot_test.png")
	if err != nil {
		t.Errorf("The created file: %v can not be found", linePlotFilename)
	}
	_, err = os.Stat("bar_plot_test.png")
	if err != nil {
		t.Errorf("The created file: %v can not be found", barPlotFilename)
	}
}

func TestFillNa(t *testing.T) {

	fillValue := 0

	t.Run("MixedDataColumn", func(t *testing.T) {
		df := goframe.NewDataFrame()
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("mixed", []any{1, nil, 3, nil, 5})))

		df.FillNa(fillValue)

		// Test column 'mixed'
		dataCol, err := df.Select("mixed")
		if err != nil {
			t.Fatalf("Failed to select 'data' column: %v", err)
		}

		// Test FillNa
		for id, value := range dataCol.Data {
			if id == 1 || id == 3 {
				if value != fillValue {
					t.Errorf("Expected fillValue: %v , got %v", fillValue, value)
				}
			}

			if value == nil {
				t.Errorf("Expected non Nil, got Nil")
			}
		}
	})

	t.Run("allNil", func(t *testing.T) {
		df := goframe.NewDataFrame()
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("allNil", []any{nil, nil, nil, nil, nil})))

		df.FillNa(fillValue)

		// Test column 'allNil'
		dataCol, err := df.Select("allNil")
		if err != nil {
			t.Fatalf("Failed to select 'data' column: %v", err)
		}

		// Test FillNa
		for _, value := range dataCol.Data {

			if value != fillValue {
				t.Errorf("Expected %v, got %v", fillValue, value)
			}
		}
	})

	t.Run("noNill", func(t *testing.T) {
		df := goframe.NewDataFrame()
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("noNill", []any{1, 2, 3, 4, 5})))

		df.FillNa(fillValue)

		// Test column 'allNil'
		dataCol, err := df.Select("noNill")
		if err != nil {
			t.Fatalf("Failed to select 'data' column: %v", err)
		}

		expected := []any{1, 2, 3, 4, 5}

		// Test FillNa
		for id, value := range dataCol.Data {

			if value != expected[id] {
				t.Errorf("Expected %v, got %v", expected[id], value)
			}
		}
	})

}

func TestDropNa(t *testing.T) {

	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("id", []int{1, 2, 3, 4, 5})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value", []any{"A", nil, "C", "D", nil})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("score", []any{90, 85, 70, nil, 95})))

	err := df.DropNa()
	if err != nil {
		t.Errorf("Failed to drop rows: %v", err)
	}

	expectedRows := 2
	if df.Nrows() != expectedRows {
		t.Errorf("Expected %d rows after DropNa, but got %d", expectedRows, df.Nrows())
	}

	idCol, err := df.Select("id")
	if err != nil {
		t.Errorf("Failed to select column: id")
	}
	valueCol, err := df.Select("value")
	if err != nil {
		t.Errorf("Failed to select column: value")
	}
	scoreCol, err := df.Select("score")
	if err != nil {
		t.Errorf("Failed to select column: score")
	}

	expectedId := []int{1, 3}
	for i, value := range idCol.Data {
		if value != expectedId[i] {
			t.Errorf("Expected id %d, but got %v", expectedId[i], value)
		}
	}

	expectedValue := []any{"A", "C"}
	for i, value := range valueCol.Data {
		if value != expectedValue[i] {
			t.Errorf("Expected id %d, but got %v", expectedValue[i], value)
		}
	}

	expectedScore := []any{90, 70}
	for i, value := range scoreCol.Data {
		if value.(int) != expectedScore[i] {
			t.Errorf("Expected id %d, but got %v", expectedScore[i], value)
		}
	}

}

func TestAstype(t *testing.T) {

	t.Run("Float64ToInt", func(t *testing.T) {
		df := goframe.NewDataFrame()
		floatCol := "floatCol"
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn(floatCol, []float64{1, 2, 3, 4, 5})))

		err := df.Astype(floatCol, "int")
		if err != nil {
			t.Fatalf("Astype failed: %v", err)
		}

		dataCol, err := df.Select(floatCol)
		if err != nil {
			t.Fatalf("Failed to select %v column: %v", floatCol, err)
		}

		// Test AsType to convert float64 to int types
		expectedValues := []int{1, 2, 3, 4, 5}
		for i, value := range dataCol.Data {
			if reflect.ValueOf(value).Kind() != reflect.Int {
				t.Errorf("Index %d: Expected int, got %v", i, reflect.ValueOf(value).Kind())
			}
			if value.(int) != expectedValues[i] {
				t.Errorf("Index %d: Expected value %v, got %v", i, expectedValues[i], value)
			}
		}
	})

	t.Run("IntToFloat64", func(t *testing.T) {
		intCol := "intCol"
		df := goframe.NewDataFrame()
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn(intCol, []int{1, 2, 3, 4, 5})))

		df.Astype(intCol, "float64")

		dataCol, err := df.Select(intCol)
		if err != nil {
			t.Fatalf("Failed to select %v column: %v", intCol, err)
		}

		// Test AsType to convert int to float64 type
		expectedValues := []float64{1, 2, 3, 4, 5}
		for i, value := range dataCol.Data {
			if reflect.ValueOf(value).Kind() != reflect.Float64 {
				t.Errorf("Index %d: Expected float64, got %v", i, reflect.ValueOf(value).Kind())
			}
			if value.(float64) != expectedValues[i] {
				t.Errorf("Index %d: Expected value %v, got %v", i, expectedValues[i], value)
			}
		}
	})

	t.Run("IntToString", func(t *testing.T) {
		intCol := "intCol"
		df := goframe.NewDataFrame()
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn(intCol, []int{10, 20, 30})))

		df.Astype(intCol, "string")

		dataCol, err := df.Select(intCol)
		if err != nil {
			t.Fatalf("Failed to select %v column: %v", intCol, err)
		}

		// Test AsType to convert int to string type
		expectedValues := []string{"10", "20", "30"}
		for i, value := range dataCol.Data {
			if reflect.ValueOf(value).Kind() != reflect.String {
				t.Errorf("Index %d: Expected string, got %v", i, reflect.ValueOf(value).Kind())
			}
			if value.(string) != expectedValues[i] {
				t.Errorf("Index %d: Expected value %v, got %v", i, expectedValues[i], value)
			}
		}
	})

	t.Run("FloatToString", func(t *testing.T) {
		df := goframe.NewDataFrame()
		floatCol := "floatCol"
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn(floatCol, []float64{1, 2, 3, 4, 5})))

		df.Astype(floatCol, "string")

		dataCol, err := df.Select(floatCol)
		if err != nil {
			t.Fatalf("Failed to select %v column: %v", floatCol, err)
		}

		// Test AsType to convert float64 to string type
		expectedValues := []string{"1", "2", "3", "4", "5"}
		for i, value := range dataCol.Data {
			if reflect.ValueOf(value).Kind() != reflect.String {
				t.Errorf("Index %d: Expected string, got %v", i, reflect.ValueOf(value).Kind())
			}
			if value.(string) != expectedValues[i] {
				t.Errorf("Index %d: Expected value %v, got %v", i, expectedValues[i], value)
			}
		}
	})

}

func TestGroupBy(t *testing.T) {

	df := goframe.NewDataFrame()

	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("dept", []string{"IT", "HR", "IT"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("score", []int{500, 300, 700})))

	keyName := "dept"
	var errors error

	grouped := df.Groupby(keyName)
	err := grouped.Error()
	if err != nil {
		t.Fatalf("An error occured: %v", err)
	}

	//create the expected data
	groups := map[any][]map[string]any{
		"HR": {
			{"dept": "HR", "score": 300},
		},
		"IT": {
			{"dept": "IT", "score": 500},
			{"dept": "IT", "score": 700},
		},
	}
	expected := goframe.GroupedDataFrame{
		Groups: groups,
		Key:    keyName,
		Err:    errors,
	}
	if expected.Error() != nil {
		t.Fatalf("An expected error has occured: %v", expected.Error())
	}

	equal := reflect.DeepEqual(expected.Groups, grouped.Groups)
	if !equal {
		t.Errorf("Grouped data does not match expected result.\nExpected: %#v\nGot: %#v", expected.Groups, grouped.Groups)
	}

	t.Run("groupByList", func(t *testing.T) {
		df := goframe.NewDataFrame()

		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Bob", "Tim", "Sam"})))
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("dept", []string{"IT", "HR", "IT"})))
		df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("salary", []int{600, 700, 600})))

		keySlice := []string{"dept", "salary"}
		var errors error

		grouped := df.Groupby(keySlice)
		err := grouped.Error()
		if err != nil {
			t.Fatalf("An error occured: %v", err)
		}

		//create the expected data
		groups := map[any][]map[string]any{
			"IT|600": {
				{"name": "Bob", "dept": "IT", "salary": 600},
				{"name": "Sam", "dept": "IT", "salary": 600},
			},
			"HR|700": {
				{"name": "Tim", "dept": "HR", "salary": 700},
			},
		}
		expected := goframe.GroupedDataFrame{
			Groups: groups,
			Key:    keyName,
			Err:    errors,
		}
		if expected.Error() != nil {
			t.Fatalf("An expected error has occured: %v", expected.Error())
		}

		equal := reflect.DeepEqual(expected.Groups, grouped.Groups)
		if !equal {
			t.Errorf("Grouped data does not match expected result.\nExpected: %#v\nGot: %#v", expected.Groups, grouped.Groups)
		}
	})

	// The subtests will be testing on the aggregate methods
	t.Run("Sum", func(t *testing.T) {
		sumDf, err := grouped.Sum("score")
		if err != nil {
			t.Fatalf("Error trying to sum groups: %v", err)
		}

		// check if sumDf is what we expected
		expectedDataframe := goframe.NewDataFrame()
		groupKeys := []any{"IT", "HR"}

		groupKeyColumn := goframe.NewColumn("GroupKey", groupKeys)
		expectedDataframe.AddColumn(groupKeyColumn)

		scores := []any{1200.0, 300.0}
		scoreColumn := goframe.NewColumn("score", scores)
		expectedDataframe.AddColumn(scoreColumn)

		match := dataFramesEqual(expectedDataframe, sumDf)
		if !match {
			t.Logf("expected data: %v", expectedDataframe.String())
			t.Logf("data obtained: %v", sumDf)
			t.Errorf("Summed data did not match expected results. \nExpected: %#v \nGot: %#v", expectedDataframe, sumDf)
		}
	})

	t.Run("SumWithoutArgs", func(t *testing.T) {
		df2 := goframe.NewDataFrame()

		df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("dept", []string{"IT", "HR", "IT"})))
		df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("score", []int{500, 300, 700})))
		df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("salary", []int{100, 200, 300})))

		keyName := "dept"

		grouped := df2.Groupby(keyName)
		err := grouped.Error()
		if err != nil {
			t.Fatalf("An error occured: %v", err)
		}

		sumDf, err := grouped.Sum()
		if err != nil {
			t.Fatalf("Error trying to sum groups: %v", err)
		}

		expectedDataframe := goframe.NewDataFrame()
		groupKeys := []any{"IT", "HR"}

		groupKeyColumn := goframe.NewColumn("GroupKey", groupKeys)
		expectedDataframe.AddColumn(groupKeyColumn)

		scores := []any{1200.0, 300.0}
		salary := []any{400.0, 200.0}

		scoreColumn := goframe.NewColumn("score", scores)
		salaryColumn := goframe.NewColumn("salary", salary)

		expectedDataframe.AddColumn(scoreColumn)
		expectedDataframe.AddColumn(salaryColumn)

		match := dataFramesEqual(expectedDataframe, sumDf)
		if !match {
			t.Logf("expected data: %v", expectedDataframe.String())
			t.Logf("data obtained: %v", sumDf)
			t.Errorf("Summed data did not match expected results. \nExpected: %#v \nGot: %#v", expectedDataframe, sumDf)
		}
	})

	t.Run("Mean", func(t *testing.T) {
		sumDf, err := grouped.Mean("score")
		if err != nil {
			t.Fatalf("Error trying to average groups: %v", err)
		}

		expectedDataframe := goframe.NewDataFrame()
		groupKeys := []any{"IT", "HR"}

		groupKeyColumn := goframe.NewColumn("GroupKey", groupKeys)
		expectedDataframe.AddColumn(groupKeyColumn)

		scores := []any{600.0, 300.0}
		scoreColumn := goframe.NewColumn("score", scores)
		expectedDataframe.AddColumn(scoreColumn)

		match := dataFramesEqual(expectedDataframe, sumDf)
		if !match {
			t.Logf("expected data: %v", expectedDataframe.String())
			t.Logf("data obtained: %v", sumDf)
			t.Errorf("Averaged data did not match expected results. \nExpected: %#v \nGot: %#v", expectedDataframe, sumDf)
		}
	})

	t.Run("Count", func(t *testing.T) {
		sumDf, err := grouped.Count("score")
		if err != nil {
			t.Fatalf("Error trying to count groups: %v", err)
		}

		// check if sumDf is what we expected
		expectedDataframe := goframe.NewDataFrame()
		groupKeys := []any{"IT", "HR"}

		groupKeyColumn := goframe.NewColumn("GroupKey", groupKeys)
		expectedDataframe.AddColumn(groupKeyColumn)

		scores := []any{2, 1}
		scoreColumn := goframe.NewColumn("score", scores)
		expectedDataframe.AddColumn(scoreColumn)

		match := dataFramesEqual(expectedDataframe, sumDf)
		if !match {
			t.Logf("expected data: %v", expectedDataframe.String())
			t.Logf("data obtained: %v", sumDf)
			t.Errorf("Averaged data did not match expected results. \nExpected: %#v \nGot: %#v", expectedDataframe, sumDf)
		}
	})
}

// Test sum on a handcrafted GroupedDataFrame (no GroupBy)
func TestSum(t *testing.T) {

	groups := map[any][]map[string]any{
		"HR": {
			{"dept": "HR", "score": 300},
		},
		"IT": {
			{"dept": "IT", "score": 500},
			{"dept": "IT", "score": 700},
		},
	}

	keyOrder := []any{
		"IT", "HR",
	}

	data := goframe.GroupedDataFrame{
		Groups:   groups,
		KeyOrder: keyOrder,
		Key:      "IT",
	}

	sumDf, err := data.Sum("score")
	if err != nil {
		t.Fatalf("Error trying to sum data: %v", err)
	}

	expectedDataframe := goframe.NewDataFrame()
	groupKeys := []any{"IT", "HR"}

	groupKeyColumn := goframe.NewColumn("GroupKey", groupKeys)
	expectedDataframe.AddColumn(groupKeyColumn)

	scores := []any{1200.0, 300.0}
	scoreColumn := goframe.NewColumn("score", scores)
	expectedDataframe.AddColumn(scoreColumn)

	match := dataFramesEqual(expectedDataframe, sumDf)
	if !match {
		t.Logf("expected data: %v", expectedDataframe.String())
		t.Logf("data obtained: %v", sumDf)
		t.Errorf("Summed data did not match expected results. \nExpected: %#v \nGot: %#v", expectedDataframe, sumDf)
	}
}

func TestMultiSelect(t *testing.T) {
	df := goframe.NewDataFrame()

	col1 := goframe.ConvertToAnyColumn(goframe.NewColumn("dept", []string{"IT", "HR", "IT"}))
	col2 := goframe.ConvertToAnyColumn(goframe.NewColumn("score", []int{500, 300, 700}))
	col3 := goframe.ConvertToAnyColumn(goframe.NewColumn("salary", []int{100, 200, 300}))

	df.AddColumn(col1)
	df.AddColumn(col2)
	df.AddColumn(col3)

	expectedDataframe := goframe.NewDataFrame()
	expectedDataframe.AddColumn(col1)
	expectedDataframe.AddColumn(col2)

	multiDf, err := df.MultiSelect("dept", "score")
	if err != nil {
		t.Errorf("An error occured trying to MultiSelect columns: %v", err)
	}

	match := dataFramesEqual(multiDf, expectedDataframe)
	if !match {
		t.Errorf("MultiSelect data did not match expected results: \nExpected: %#v \nGot: %#v", expectedDataframe, multiDf)
	}

	emptyDf, err := df.MultiSelect()
	expectedDataframe2 := goframe.NewDataFrame()
	match2 := dataFramesEqual(emptyDf, expectedDataframe2)
	if !match2 {
		t.Errorf("MultiSelect data did not match expected results: \nExpected: %#v \nGot: %#v", expectedDataframe, multiDf)
	}

}
func TestAdd(t *testing.T) {
	t.Run("Basic numeric addition", func(t *testing.T) {
		df1 := goframe.NewDataFrame()
		df2 := goframe.NewDataFrame()

		col1 := goframe.ConvertToAnyColumn(goframe.NewColumn("intCol", []int{1, 2, 3}))
		col2 := goframe.ConvertToAnyColumn(goframe.NewColumn("floatCol", []float64{1.1, 2.2, 3.3}))

		col3 := goframe.ConvertToAnyColumn(goframe.NewColumn("intCol", []int{4, 5, 6}))
		col4 := goframe.ConvertToAnyColumn(goframe.NewColumn("floatCol", []float64{4.4, 5.5, 6.6}))

		df1.AddColumn(col1)
		df1.AddColumn(col2)
		df2.AddColumn(col3)
		df2.AddColumn(col4)

		expected := goframe.NewDataFrame()
		expected.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("intCol", []any{5, 7, 9})))
		expected.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("floatCol", []any{5.5, 7.7, 9.9})))

		result, err := df1.Add(df2)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !dataFramesEqual(result, expected) {
			t.Errorf("Basic numeric addition failed.\nExpected:\n%v\nGot:\n%v", expected.String(), result.String())
		}
	})

	t.Run("String addition", func(t *testing.T) {
		df1 := goframe.NewDataFrame()
		df2 := goframe.NewDataFrame()

		col1 := goframe.ConvertToAnyColumn(goframe.NewColumn("text", []string{"a", "b", "c"}))
		col2 := goframe.ConvertToAnyColumn(goframe.NewColumn("text", []string{"x", "y", "z"}))

		df1.AddColumn(col1)
		df2.AddColumn(col2)

		expected := goframe.NewDataFrame()
		expected.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("text", []any{nil, nil, nil}))) // mimic pandas behavior

		result, err := df1.Add(df2)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !dataFramesEqual(result, expected) {
			t.Errorf("String addition failed.\nExpected:\n%v\nGot:\n%v", expected.String(), result.String())
		}
	})

	t.Run("Numerical strings addition", func(t *testing.T) {
		df1 := goframe.NewDataFrame()
		df2 := goframe.NewDataFrame()

		col1 := goframe.ConvertToAnyColumn(goframe.NewColumn("numStr", []string{"1.1", "2.2", "3.3"}))
		col2 := goframe.ConvertToAnyColumn(goframe.NewColumn("numStr", []string{"4.4", "5.5", "6.6"}))

		df1.AddColumn(col1)
		df2.AddColumn(col2)

		expected := goframe.NewDataFrame()
		expected.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("numStr", []any{5.5, 7.7, 9.9}))) // unless you parse strings to float

		result, err := df1.Add(df2)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !dataFramesEqual(result, expected) {
			t.Errorf("Numerical string addition failed.\nExpected:\n%v\nGot:\n%v", expected.String(), result.String())
		}
	})
}

/*
The dataFramesEqual function checks if the data values are numerically equal in 2 different dataframes by converting both
datatypes into float64 before comparing them.

Parameters:
  - dataframeA: The first dataframe to be compared to.
  - dataframeB: The second dataframe to be compared with.

Returns:
  - Boolean: Returns true if it numerically matches, else false.
*/
func dataFramesEqual(a, b *goframe.DataFrame) bool {

	if len(a.Columns) != len(b.Columns) {
		return false
	}

	for name, colA := range a.Columns {
		colB, ok := b.Columns[name]
		if !ok {
			return false
		}

		if len(colA.Data) != len(colB.Data) {
			return false
		}

		for i := range colA.Data {
			aVal := colA.Data[i]
			bVal := colB.Data[i]

			switch aVal.(type) {
			case float64:

				// Handle all numeric comparisons
				if almostEqual(aVal, bVal) {
					continue
				}

			}

			if !reflect.DeepEqual(aVal, bVal) {
				return false
			}
		}
	}
	fmt.Println("all floats equal within tolerance.")
	return true
}

const floatTolerance = 1e-9

func almostEqual(a, b any) bool {
	aFloat, aOk := toFloat(a)
	bFloat, bOk := toFloat(b)
	if aOk && bOk {
		return math.Abs(aFloat-bFloat) < floatTolerance
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err == nil {
			return f, true
		}
	default:
		return 0, false
	}
	return 0, false
}
