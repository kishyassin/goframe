package goframe_test

import (
	"fmt"

	goframe "github.com/kishyassin/goframe"
)

// ExampleNewDataFrame demonstrates how to create a new DataFrame.
func ExampleNewDataFrame() {
	df := goframe.NewDataFrame()
	fmt.Println(df)
	// Output: Empty DataFrame
}

// ExampleDataFrame_AddColumn demonstrates how to add a column to a DataFrame.
func ExampleDataFrame_AddColumn() {
	df := goframe.NewDataFrame()
	col := &goframe.Column[any]{
		Name: "exampleColumn",
		Data: []any{1, 2, 3},
	}
	df.AddColumn(col)
	fmt.Println(df.ColumnNames())
	// Output: [exampleColumn]
}

// ExampleDataFrame_Filter demonstrates how to filter rows in a DataFrame.
func ExampleDataFrame_Filter() {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("numbers", []int{1, 2, 3, 4, 5})))
	filtered := df.Filter(func(row map[string]any) bool {
		return row["numbers"].(int) > 3
	})
	fmt.Println(filtered.Nrows())
	// Output: 2
}

// ExampleDataFrame_FromCSV demonstrates how to create a DataFrame from a CSV file.
func ExampleDataFrame_FromCSV() {
	df := goframe.NewDataFrame()
	// Assuming a CSV file "example.csv" exists with appropriate data.
	df, err := df.FromCSV("example.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println(df)
}

// ExampleSeries demonstrates how to create and use a Series.
func ExampleSeries() {
	series := goframe.NewSeries("exampleSeries", []interface{}{1, 2, 3, 4, 5})
	fmt.Println(series.Name)
	fmt.Println(series.Len())
	fmt.Println(series.At(2))
	// Output:
	// exampleSeries
	// 5
	// 3
}
