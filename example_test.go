package goframe_test

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kishyassin/goframe"
)

func ExampleDataFrame_basic() {
	// Create a new DataFrame
	df := goframe.NewDataFrame()

	// Add some columns
	names := goframe.NewSeries("name", []interface{}{"Alice", "Bob", "Charlie"})
	ages := goframe.NewSeries("age", []interface{}{25.0, 30.0, 35.0})
	salaries := goframe.NewSeries("salary", []interface{}{50000.0, 60000.0, 70000.0})

	df.AddColumn(names)
	df.AddColumn(ages)
	df.AddColumn(salaries)

	fmt.Printf("DataFrame has %d rows and %d columns\n", df.Nrows(), df.Ncols())
	fmt.Printf("Column names: %v\n", df.ColumnNames())

	// Output:
	// DataFrame has 3 rows and 3 columns
	// Column names: [name age salary]
}

func ExampleDataFrame_Select() {
	// Create DataFrame with sample data
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.NewSeries("name", []interface{}{"Alice", "Bob", "Charlie"}))
	df.AddColumn(goframe.NewSeries("age", []interface{}{25.0, 30.0, 35.0}))

	// Select a column by name
	ageColumn, err := df.Select("age")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Selected column: %s\n", ageColumn.Name)
	fmt.Printf("First age value: %v\n", ageColumn.At(0))

	// Output:
	// Selected column: age
	// First age value: 25
}

func ExampleDataFrame_Filter() {
	// Create DataFrame with sample data
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.NewSeries("name", []interface{}{"Alice", "Bob", "Charlie", "David"}))
	df.AddColumn(goframe.NewSeries("age", []interface{}{25.0, 30.0, 35.0, 40.0}))

	// Filter rows where age > 30
	filtered := df.Filter(func(row []interface{}) bool {
		age, ok := row[1].(float64) // age is the second column
		return ok && age > 30
	})

	fmt.Printf("Original rows: %d\n", df.Nrows())
	fmt.Printf("Filtered rows: %d\n", filtered.Nrows())

	// Output:
	// Original rows: 4
	// Filtered rows: 2
}

func ExampleSeries_stats() {
	// Create a series with numeric data
	numbers := goframe.NewSeries("values", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0})

	mean, _ := numbers.Mean()
	sum, _ := numbers.Sum()
	min, _ := numbers.Min()
	max, _ := numbers.Max()

	fmt.Printf("Mean: %.1f\n", mean)
	fmt.Printf("Sum: %.1f\n", sum)
	fmt.Printf("Min: %.1f\n", min)
	fmt.Printf("Max: %.1f\n", max)

	// Output:
	// Mean: 3.0
	// Sum: 15.0
	// Min: 1.0
	// Max: 5.0
}

func ExampleFromCSVReader() {
	// Sample CSV data
	csvData := `name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000`

	// Create DataFrame from CSV
	reader := strings.NewReader(csvData)
	df, err := goframe.FromCSVReader(reader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Loaded DataFrame: %d rows, %d columns\n", df.Nrows(), df.Ncols())
	fmt.Printf("Columns: %v\n", df.ColumnNames())

	// Calculate mean salary
	salaryCol, _ := df.Select("salary")
	meanSalary, _ := salaryCol.Mean()
	fmt.Printf("Mean salary: %.0f\n", meanSalary)

	// Output:
	// Loaded DataFrame: 3 rows, 3 columns
	// Columns: [name age salary]
	// Mean salary: 60000
}

func ExampleDataFrame_ToCSVWriter() {
	// Create a DataFrame
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.NewSeries("product", []interface{}{"A", "B", "C"}))
	df.AddColumn(goframe.NewSeries("price", []interface{}{10.0, 20.0, 30.0}))
	df.AddColumn(goframe.NewSeries("quantity", []interface{}{100.0, 200.0, 150.0}))

	// Export to CSV string (using strings.Builder as writer)
	var output strings.Builder
	err := df.ToCSVWriter(&output)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(output.String())

	// Output:
	// product,price,quantity
	// A,10,100
	// B,20,200
	// C,30,150
}

func ExampleFromCSV() {
	// This example shows how to load from an actual CSV file
	// First, create a temporary CSV file for demonstration
	tmpFile, err := os.CreateTemp("", "example*.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up

	// Write sample data to the file
	csvContent := `item,value,category
laptop,1000,electronics
book,20,education
pen,2,office`

	if _, err := tmpFile.WriteString(csvContent); err != nil {
		log.Fatal(err)
	}
	tmpFile.Close()

	// Load DataFrame from CSV file
	df, err := goframe.FromCSV(tmpFile.Name())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Loaded %d rows and %d columns\n", df.Nrows(), df.Ncols())

	// Calculate statistics on the value column
	valueCol, _ := df.Select("value")
	total, _ := valueCol.Sum()
	average, _ := valueCol.Mean()

	fmt.Printf("Total value: %.0f\n", total)
	fmt.Printf("Average value: %.1f\n", average)

	// Output:
	// Loaded 3 rows and 3 columns
	// Total value: 1022
	// Average value: 340.7
}
