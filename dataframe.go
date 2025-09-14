package goframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// DataFrame represents a collection of typed columns
type DataFrame struct {
	Columns map[string]*Column[any] // Map column name to generic Column
}

// NewDataFrame creates a new empty DataFrame
func NewDataFrame() *DataFrame {
	return &DataFrame{
		Columns: make(map[string]*Column[any]),
	}
}

// AddColumn adds a new column to the DataFrame
func (df *DataFrame) AddColumn(col *Column[any]) error {
	if _, exists := df.Columns[col.Name]; exists {
		return fmt.Errorf("column '%s' already exists", col.Name)
	}
	df.Columns[col.Name] = col
	return nil
}

// DropColumn removes a column from the DataFrame
func (df *DataFrame) DropColumn(name string) error {
	if _, exists := df.Columns[name]; !exists {
		return fmt.Errorf("column '%s' does not exist", name)
	}

	delete(df.Columns, name)
	return nil
}

// Nrows returns the number of rows in the DataFrame
func (df *DataFrame) Nrows() int {
	for _, col := range df.Columns {
		return col.Len() // Return the length of the first column
	}
	return 0 // Return 0 if there are no columns
}

// Ncols returns the number of columns in the DataFrame
func (df *DataFrame) Ncols() int {
	return len(df.Columns)
}

// ColumnNames returns the names of all columns in the DataFrame
func (df *DataFrame) ColumnNames() []string {
	names := make([]string, 0, len(df.Columns))
	for name := range df.Columns {
		names = append(names, name)
	}
	sort.Strings(names) // Ensure consistent order
	return names
}

// Select returns a column by name
func (df *DataFrame) Select(name string) (*Column[any], error) {
	col, exists := df.Columns[name]
	if !exists {
		return nil, fmt.Errorf("column '%s' does not exist", name)
	}
	return col, nil
}

// Row returns a row by index
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

// Filter returns a new DataFrame with rows that satisfy the given condition
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

// FromCSV creates a DataFrame from a CSV file
func FromCSV(filename string) (*DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	return FromCSVReader(file)
}

// FromCSVReader creates a DataFrame from a CSV reader
func FromCSVReader(reader io.Reader) (*DataFrame, error) {
	csvReader := csv.NewReader(reader)

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}

	// Initialize DataFrame with columns
	df := NewDataFrame()
	for _, colName := range header {
		df.Columns[colName] = &Column[any]{
			Name: colName,
			Data: []any{},
		}
	}

	// Read data rows
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading row: %w", err)
		}

		if len(record) != len(header) {
			return nil, fmt.Errorf("row has %d columns, expected %d", len(record), len(header))
		}

		// Add data to each column, trying to parse as number if possible
		for i, value := range record {
			col := df.Columns[header[i]]
			if floatVal, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
				col.Data = append(col.Data, floatVal)
			} else {
				col.Data = append(col.Data, strings.TrimSpace(value))
			}
		}
	}

	return df, nil
}

// ToCSV exports the DataFrame to a CSV file
func (df *DataFrame) ToCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	return df.ToCSVWriter(file)
}

// ToCSVWriter exports the DataFrame to a CSV writer
func (df *DataFrame) ToCSVWriter(writer io.Writer) error {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// Write header
	header := df.ColumnNames()
	if err := csvWriter.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write rows
	for i := 0; i < df.Nrows(); i++ {
		row := make([]string, len(header))
		for idx, colName := range header {
			value, err := df.Columns[colName].At(i)
			if err != nil {
				return fmt.Errorf("error accessing value: %w", err)
			}
			row[idx] = fmt.Sprintf("%v", value)
		}
		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("error writing row: %w", err)
		}
	}

	return nil
}

// String returns a string representation of the DataFrame
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

// Head returns the first n rows of the DataFrame
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

// Tail returns the last n rows of the DataFrame
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

// AppendRow adds a new row to the DataFrame
func (df *DataFrame) AppendRow(row map[string]any) error {
	for name, value := range row {
		col, exists := df.Columns[name]
		if !exists {
			return fmt.Errorf("column '%s' does not exist", name)
		}
		col.Data = append(col.Data, value)
	}
	return nil
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

// Mean calculates the mean of numeric values for each column in the DataFrame
func (df *DataFrame) Mean() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		mean, err := series.Mean()
		if err != nil {
			return nil, fmt.Errorf("error calculating mean for column '%s': %w", name, err)
		}
		results[name] = mean
	}
	return results, nil
}

// Sum calculates the sum of numeric values for each column in the DataFrame
func (df *DataFrame) Sum() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		sum, err := series.Sum()
		if err != nil {
			return nil, fmt.Errorf("error calculating sum for column '%s': %w", name, err)
		}
		results[name] = sum
	}
	return results, nil
}

// Min calculates the minimum value for each column in the DataFrame
func (df *DataFrame) Min() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		min, err := series.Min()
		if err != nil {
			return nil, fmt.Errorf("error calculating min for column '%s': %w", name, err)
		}
		results[name] = min
	}
	return results, nil
}

// Max calculates the maximum value for each column in the DataFrame
func (df *DataFrame) Max() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		max, err := series.Max()
		if err != nil {
			return nil, fmt.Errorf("error calculating max for column '%s': %w", name, err)
		}
		results[name] = max
	}
	return results, nil
}

// Join combines two DataFrames based on a key column and join type (inner, left, right, outer).
func (df *DataFrame) Join(other *DataFrame, key string, joinType string) (*DataFrame, error) {
	if _, exists := df.Columns[key]; !exists {
		return nil, fmt.Errorf("key column '%s' does not exist in the first DataFrame", key)
	}
	if _, exists := other.Columns[key]; !exists {
		return nil, fmt.Errorf("key column '%s' does not exist in the second DataFrame", key)
	}

	result := NewDataFrame()

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

	// Perform the join based on the join type
	switch joinType {
	case "inner":
		for i := 0; i < df.Nrows(); i++ {
			rowA, _ := df.Row(i)
			for j := 0; j < other.Nrows(); j++ {
				rowB, _ := other.Row(j)
				if rowA[key] == rowB[key] {
					mergedRow := mergeRows(rowA, rowB)
					appendRowToDataFrame(result, mergedRow)
				}
			}
		}
	case "left":
		for i := 0; i < df.Nrows(); i++ {
			rowA, _ := df.Row(i)
			matched := false
			for j := 0; j < other.Nrows(); j++ {
				rowB, _ := other.Row(j)
				if rowA[key] == rowB[key] {
					mergedRow := mergeRows(rowA, rowB)
					appendRowToDataFrame(result, mergedRow)
					matched = true
				}
			}
			if !matched {
				appendRowToDataFrame(result, rowA)
			}
		}
	case "right":
		for i := 0; i < other.Nrows(); i++ {
			rowB, _ := other.Row(i)
			matched := false
			for j := 0; j < df.Nrows(); j++ {
				rowA, _ := df.Row(j)
				if rowB[key] == rowA[key] {
					mergedRow := mergeRows(rowA, rowB)
					appendRowToDataFrame(result, mergedRow)
					matched = true
				}
			}
			if !matched {
				appendRowToDataFrame(result, rowB)
			}
		}
	case "outer":
		matchedRows := make(map[any]bool)
		for i := 0; i < df.Nrows(); i++ {
			rowA, _ := df.Row(i)
			matched := false
			for j := 0; j < other.Nrows(); j++ {
				rowB, _ := other.Row(j)
				if rowA[key] == rowB[key] {
					mergedRow := mergeRows(rowA, rowB)
					appendRowToDataFrame(result, mergedRow)
					matchedRows[rowA[key]] = true
					matched = true
				}
			}
			if !matched {
				appendRowToDataFrame(result, rowA)
			}
		}
		for i := 0; i < other.Nrows(); i++ {
			rowB, _ := other.Row(i)
			if !matchedRows[rowB[key]] {
				appendRowToDataFrame(result, rowB)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported join type: %s", joinType)
	}

	return result, nil
}

// mergeRows merges two rows into one
func mergeRows(rowA, rowB map[string]any) map[string]any {
	merged := make(map[string]any)
	for k, v := range rowA {
		merged[k] = v
	}
	for k, v := range rowB {
		if _, exists := merged[k]; !exists {
			merged[k] = v
		}
	}
	return merged
}

// appendRowToDataFrame appends a row to a DataFrame
func appendRowToDataFrame(df *DataFrame, row map[string]any) {
	for name, value := range row {
		df.Columns[name].Data = append(df.Columns[name].Data, value)
	}
}

// Column represents a typed column in the DataFrame
// T is the type of the column data (e.g., int, float64, string, bool)
type Column[T any] struct {
	Name string
	Data []T
}

// NewColumn creates a new typed column
func NewColumn[T any](name string, data []T) *Column[T] {
	return &Column[T]{
		Name: name,
		Data: data,
	}
}

// Len returns the length of the column
func (c *Column[T]) Len() int {
	return len(c.Data)
}

// At returns the value at the given index
func (c *Column[T]) At(index int) (T, error) {
	if index < 0 || index >= len(c.Data) {
		var zero T
		return zero, fmt.Errorf("index out of bounds")
	}
	return c.Data[index], nil
}

// ConvertToAnyColumn converts a typed column to a generic column of type `any`
func ConvertToAnyColumn[T any](col *Column[T]) *Column[any] {
	genericData := make([]any, len(col.Data))
	for i, v := range col.Data {
		genericData[i] = v
	}
	return &Column[any]{
		Name: col.Name,
		Data: genericData,
	}
}
