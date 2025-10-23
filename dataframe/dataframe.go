// Package goframe provides a simple and flexible framework for working with tabular data in Go.
// It includes support for creating, manipulating, and analyzing data frames, as well as exporting
// and importing data from CSV files. The package is designed to be type-safe and easy to use,
// making it suitable for data analysis, machine learning, and general data processing tasks.

package goframe

import (
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
)

// DataFrame represents a collection of typed columns.
// It provides methods for adding, removing, and manipulating columns and rows.
type DataFrame struct {
	Columns map[string]*Column[any] // Map column name to generic Column
}

// NewDataFrame creates a new empty DataFrame.
//
// Returns:
//   - *DataFrame: A pointer to the newly created DataFrame.
func NewDataFrame() *DataFrame {
	return &DataFrame{
		Columns: make(map[string]*Column[any]),
	}
}

// Nrows returns the number of rows in the DataFrame.
//
// Returns:
//   - int: The number of rows in the DataFrame.
func (df *DataFrame) Nrows() int {
	for _, col := range df.Columns {
		return col.Len() // Return the length of the first column
	}
	return 0 // Return 0 if there are no columns
}

// Ncols returns the number of columns in the DataFrame.
//
// Returns:
//   - int: The number of columns in the DataFrame.
func (df *DataFrame) Ncols() int {
	return len(df.Columns)
}

// Select returns a column by name.
//
// Parameters:
//   - name: The name of the column to select.
//
// Returns:
//   - *Column[any]: The selected column.
//   - error: An error if the column does not exist.
func (df *DataFrame) Select(name string) (*Column[any], error) {
	col, exists := df.Columns[name]
	if !exists {
		return nil, fmt.Errorf("column '%s' does not exist", name)
	}
	return col, nil
}

// MultiSelect returns a dataframe of the selected columns.
//
// Parameters:
//   - name: The name of the column(s) to select.
//
// Returns:
//   - *DataFrame: The DataFrame struct containing the selected columns.
//   - error: An error if the column(s) does not exist.
func (df *DataFrame) MultiSelect(name ...string) (*DataFrame, error) {
	newDf := DataFrame{}
	newDf.Columns = make(map[string]*Column[any])

	if len(name) < 1 {
		return &newDf, fmt.Errorf("Please enter 1 or more column name(s)")
	}

	for _, name := range name {

		col, exists := df.Columns[name]

		if !exists {
			return nil, fmt.Errorf("column '%s' does not exist", name)
		}

		AddTypedColumn(&newDf, col)
	}

	return &newDf, nil
}

// Row returns a row by index.
//
// Parameters:
//   - index: The index of the row to retrieve.
//
// Returns:
//   - map[string]any: A map representing the row, with column names as keys.
//   - error: An error if the index is out of bounds.
func (df *DataFrame) Row(index int) (map[string]any, error) {
	if index < 0 || index >= df.Nrows() {
		return nil, fmt.Errorf("index out of bounds")
	}

	row := make(map[string]any)
	for name, col := range df.Columns {
		value, err := col.At(index)
		if err != nil {
			return nil, fmt.Errorf("error accessing column '%s': %w", name, err)
		}
		row[name] = value
	}
	return row, nil
}

// Filter returns a new DataFrame with rows that satisfy the given condition.
//
// Parameters:
//   - condition: A function that takes a row and returns true if the row should be included.
//
// Returns:
//   - *DataFrame: A new DataFrame containing the filtered rows.
func (df *DataFrame) Filter(condition func(row map[string]any) bool) *DataFrame {
	filtered := NewDataFrame()

	// Initialize new columns
	for name := range df.Columns {
		filtered.Columns[name] = &Column[any]{
			Name: name,
			Data: []any{},
		}
	}

	// Iterate through rows and apply the condition
	for i := 0; i < df.Nrows(); i++ {
		row, err := df.Row(i)
		if err != nil {
			continue
		}
		if condition(row) {
			for name, value := range row {
				filtered.Columns[name].Data = append(filtered.Columns[name].Data, value)
			}
		}
	}

	return filtered
}

// String returns a string representation of the DataFrame.
//
// Returns:
//   - string: A string representation of the DataFrame.
func (df *DataFrame) String() string {
	if df.Nrows() == 0 {
		return "Empty DataFrame"
	}

	var result strings.Builder

	// Header
	result.WriteString(fmt.Sprintf("DataFrame (%d rows x %d columns)\n", df.Nrows(), df.Ncols()))

	// Column names
	header := df.ColumnNames()
	result.WriteString(strings.Join(header, "\t"))
	result.WriteString("\n")

	// First few rows (max 10)
	maxRows := 10
	if df.Nrows() < maxRows {
		maxRows = df.Nrows()
	}

	for i := 0; i < maxRows; i++ {
		row := make([]string, len(header))
		for idx, colName := range header {
			value, err := df.Columns[colName].At(i)
			if err != nil {
				row[idx] = "<error>"
			} else {
				row[idx] = fmt.Sprintf("%v", value)
			}
		}
		result.WriteString(strings.Join(row, "\t"))
		result.WriteString("\n")
	}

	if df.Nrows() > maxRows {
		result.WriteString("...\n")
	}

	return result.String()
}

// Head returns the first n rows of the DataFrame.
//
// Parameters:
//   - n: The number of rows to return.
//
// Returns:
//   - *DataFrame: A new DataFrame containing the first n rows.
func (df *DataFrame) Head(n int) *DataFrame {
	if n > df.Nrows() {
		n = df.Nrows()
	}

	head := NewDataFrame()
	for name, col := range df.Columns {
		newCol := &Column[any]{
			Name: name,
			Data: col.Data[:n],
		}
		head.Columns[name] = newCol
	}
	return head
}

// Tail returns the last n rows of the DataFrame.
//
// Parameters:
//   - n: The number of rows to return.
//
// Returns:
//   - *DataFrame: A new DataFrame containing the last n rows.
func (df *DataFrame) Tail(n int) *DataFrame {
	totalRows := df.Nrows()
	if n > totalRows {
		n = totalRows
	}

	tail := NewDataFrame()
	for name, col := range df.Columns {
		newCol := &Column[any]{
			Name: name,
			Data: col.Data[totalRows-n:],
		}
		tail.Columns[name] = newCol
	}
	return tail
}

// DropRow removes a row by index from the DataFrame
func (df *DataFrame) DropRow(i int) error {
	if i < 0 || i >= df.Nrows() {
		return fmt.Errorf("index out of bounds")
	}

	for _, col := range df.Columns {
		col.Data = append(col.Data[:i], col.Data[i+1:]...)
	}
	return nil
}

func checkExists(df *DataFrame, other *DataFrame, key string) error {
	if _, exists := df.Columns[key]; !exists {
		return fmt.Errorf("key column '%s' does not exist in the first DataFrame", key)
	}
	if _, exists := other.Columns[key]; !exists {
		return fmt.Errorf("key column '%s' does not exist in the second DataFrame", key)
	}

	return nil
}

func appendCols(df *DataFrame, other *DataFrame, result *DataFrame) error {
	// Add columns from both DataFrames to the result
	for name := range df.Columns {
		result.Columns[name] = &Column[any]{
			Name: name,
			Data: []any{},
		}
	}
	for name := range other.Columns {
		if _, exists := result.Columns[name]; !exists {
			result.Columns[name] = &Column[any]{
				Name: name,
				Data: []any{},
			}
		}
	}

	return nil
}

// mergeRows merges two rows into one
func mergeRows(rowA, rowB map[string]any) map[string]any {
	merged := make(map[string]any)
	maps.Copy(merged, rowA)
	for id, v := range rowB {
		if _, exists := merged[id]; !exists {
			merged[id] = v
		}
	}
	return merged
}

func (df *DataFrame) AppendRow(result *DataFrame, row map[string]any) error {

	// Add new columns if they don't exist.
	for name := range row {
		if _, exists := result.Columns[name]; !exists {
			newCol := NewColumn(name, make([]any, 0))
			// add the new column to the result dataframe
			err := result.AddColumn(ConvertToAnyColumn(newCol))
			if err != nil {
				return fmt.Errorf("error adding column: %v", err)
			}
		}
	}

	// In the new Columns, put nil placeholders
	for name, col := range result.Columns {
		if _, exists := row[name]; !exists {
			// Append a nil value if the new row doesn't have data for this column.
			col.Data = append(col.Data, nil)
		}
	}

	// Append the new row's data.
	for name, value := range row {
		result.Columns[name].Data = append(result.Columns[name].Data, value)
	}

	return nil

}

// ColumnNames returns the names of all columns in the DataFrame.
//
// Returns:
//   - []string: A sorted list of column names.
func (df *DataFrame) ColumnNames() []string {
	names := make([]string, 0, len(df.Columns))
	for name := range df.Columns {
		names = append(names, name)
	}
	sort.Strings(names) // Ensure consistent order
	return names
}

// RenameColumn renames a column in the DataFrame
func (df *DataFrame) RenameColumn(oldName, newName string) error {
	col, exists := df.Columns[oldName]
	if !exists {
		return fmt.Errorf("column '%s' does not exist", oldName)
	}
	if _, exists := df.Columns[newName]; exists {
		return fmt.Errorf("column '%s' already exists", newName)
	}

	col.Name = newName
	df.Columns[newName] = col
	delete(df.Columns, oldName)
	return nil
}

// AddColumn adds a generic column to the DataFrame.
//
// Parameters:
//   - col: The generic column to add.
//
// Returns:
//   - error: An error if the operation fails.
func (df *DataFrame) AddColumn(col *Column[any]) error {
	_, exists := df.Columns[col.Name]
	if exists {
		return fmt.Errorf("Column '%v' already exists", col.Name)
	}

	df.Columns[col.Name] = col
	return nil
}

// DropColumn removes a column from the DataFrame.
//
// Parameters:
//   - name: The name of the column to remove.
//
// Returns:
//   - error: An error if the column does not exist.
func (df *DataFrame) DropColumn(name string) error {
	if _, exists := df.Columns[name]; !exists {
		return fmt.Errorf("column '%s' does not exist", name)
	}

	delete(df.Columns, name)
	return nil
}

// NewColumn creates a new typed column
func NewColumn[T any](name string, data []T) *Column[T] {
	return &Column[T]{
		Name: name,
		Data: data,
	}
}

// Add sums 2 dataframes together.
//
// Parameters:
//   - other: The other dataframe to be summed with.
//   - fillValue: The value to fill if a Nil value is present in the dataframe.
//
// Returns:
//   - *Dataframe: The pointer to a new Dataframe that contains the summed values.
//
// Note:
//
//	If there is a Nil values detected for summing numbers in any row, the value will be defaulted to 0.
func (df *DataFrame) Add(other *DataFrame, fillValue any) (*DataFrame, error) {
	newDf := *NewDataFrame()
	if df.Ncols() != other.Ncols() {
		return &newDf, fmt.Errorf("The number of columns does not match for both dataframes. First dataframe has: %v while second dataframe has: %v", df.Ncols(), other.Ncols())
	}
	if df.Nrows() != other.Nrows() {
		return &newDf, fmt.Errorf("The number of rows does not match for both dataframes. First dataframe has: %v while second dataframe has: %v", df.Nrows(), other.Nrows())

	}
	for colName, col := range df.Columns {

		// create the new column in newDf
		colToAdd := NewColumn(colName, []any{})

		// get the other column's row data
		otherRows := other.Columns[colName].Data
		dfRows := col.Data

		// get the max number of rows between the 2
		maxNoRows := max(len(dfRows), len(otherRows))
		for i := range maxNoRows {

			val1 := dfRows[i]
			val2 := otherRows[i]
			var sum any

			// if they are not equal , try to convert them into string and concat them
			if reflect.TypeOf(val1) != reflect.TypeOf(val2) {

				str1 := fmt.Sprintf("%v", val1)
				str2 := fmt.Sprintf("%v", val2)
				sum = str1 + str2

			} else {
				// else if they are equal, just add/concat them
				switch v := val1.(type) {

				case float64:
					sum = v + val2.(float64)

				case int:
					sum = v + val2.(int)

				case string:
					sum = v + val2.(string)

				default:
					return &newDf, fmt.Errorf("Unable to sum dataframes, Unknown DataType: %v in col: %v, row: %v", v, colName, i)
				}

			}
			// Type assert both values to float64 for numeric addition

			colToAdd.Data = append(colToAdd.Data, sum)
		}
		newDf.AddColumn(colToAdd)
	}
	return &newDf, nil

}
