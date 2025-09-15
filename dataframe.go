// Package goframe provides a simple and flexible framework for working with tabular data in Go.
// It includes support for creating, manipulating, and analyzing data frames, as well as exporting
// and importing data from CSV files. The package is designed to be type-safe and easy to use,
// making it suitable for data analysis, machine learning, and general data processing tasks.

package goframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wcharczuk/go-chart/v2"
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

// AddTypedColumn adds a typed column to the DataFrame.
//
// Parameters:
//   - df: The DataFrame to which the column will be added.
//   - col: The typed column to add.
//
// Returns:
//   - error: An error if the operation fails.
func AddTypedColumn[T any](df *DataFrame, col *Column[T]) error {
	// Automatically convert the column to *Column[any]
	anyCol := ConvertToAnyColumn(col)
	return df.AddColumn(anyCol)
}

// AddColumn adds a generic column to the DataFrame.
//
// Parameters:
//   - col: The generic column to add.
//
// Returns:
//   - error: An error if the operation fails.
func (df *DataFrame) AddColumn(col *Column[any]) error {
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

// FromCSV creates a DataFrame from a CSV file.
//
// Parameters:
//   - filename: The path to the CSV file.
//
// Returns:
//   - *DataFrame: The created DataFrame.
//   - error: An error if the file cannot be read.
func FromCSV(filename string) (*DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	return FromCSVReader(file)
}

// FromCSVReader creates a DataFrame from a CSV reader.
//
// Parameters:
//   - reader: An io.Reader for the CSV data.
//
// Returns:
//   - *DataFrame: The created DataFrame.
//   - error: An error if the data cannot be read.
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

// ToCSV exports the DataFrame to a CSV file.
//
// Parameters:
//   - filename: The path to the output CSV file.
//
// Returns:
//   - error: An error if the file cannot be written.
func (df *DataFrame) ToCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	return df.ToCSVWriter(file)
}

// ToCSVWriter exports the DataFrame to a CSV writer.
//
// Parameters:
//   - writer: An io.Writer for the CSV data.
//
// Returns:
//   - error: An error if the data cannot be written.
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
				rowB, _ := other.Row(j) // Ensure rowB is defined
				if reflect.DeepEqual(rowA[key], rowB[key]) {
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
			if _, exists := matchedRows[rowB[key]]; !exists {
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

// Advanced Indexing

// MultiIndex represents hierarchical indexing for rows
type MultiIndex struct {
	Levels [][]any
	Labels [][]int
}

// BooleanIndex filters rows based on a boolean condition
func (df *DataFrame) BooleanIndex(condition func(row map[string]any) bool) *DataFrame {
	return df.Filter(condition)
}

// Loc selects rows and columns by labels
func (df *DataFrame) Loc(rowLabels []any, colLabels []string) (*DataFrame, error) {
	result := NewDataFrame()

	for _, col := range colLabels {
		if _, exists := df.Columns[col]; !exists {
			return nil, fmt.Errorf("column '%s' does not exist", col)
		}
		result.Columns[col] = &Column[any]{
			Name: col,
			Data: []any{},
		}
	}

	indexCol, indexExists := df.Columns["index"]
	if !indexExists {
		return nil, fmt.Errorf("'index' column does not exist")
	}

	for i := 0; i < df.Nrows(); i++ {
		row, _ := df.Row(i)
		for _, label := range rowLabels {
			if indexCol.Data[i] == label {
				for _, col := range colLabels {
					result.Columns[col].Data = append(result.Columns[col].Data, row[col])
				}
			}
		}
	}

	return result, nil
}

// Iloc selects rows and columns by integer positions
func (df *DataFrame) Iloc(rowIndices []int, colIndices []int) (*DataFrame, error) {
	result := NewDataFrame()
	colNames := df.ColumnNames()

	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= len(colNames) {
			return nil, fmt.Errorf("column index out of bounds")
		}
		colName := colNames[colIdx]
		result.Columns[colName] = &Column[any]{
			Name: colName,
			Data: []any{},
		}
	}

	for _, rowIdx := range rowIndices {
		if rowIdx < 0 || rowIdx >= df.Nrows() {
			return nil, fmt.Errorf("row index out of bounds")
		}
		row, _ := df.Row(rowIdx)
		for _, colIdx := range colIndices {
			colName := colNames[colIdx]
			result.Columns[colName].Data = append(result.Columns[colName].Data, row[colName])
		}
	}

	return result, nil
}

// Data Cleaning

// FillNa fills missing values in the DataFrame with a specified value
func (df *DataFrame) FillNa(value any) {
	for _, col := range df.Columns {
		for i, v := range col.Data {
			if v == nil {
				col.Data[i] = value
			}
		}
	}
}

// DropNa removes rows with missing values from the DataFrame
func (df *DataFrame) DropNa() {
	rowsToKeep := []int{}

	for i := 0; i < df.Nrows(); i++ {
		row, _ := df.Row(i)
		hasNa := false
		for _, v := range row {
			if v == nil {
				hasNa = true
				break
			}
		}
		if !hasNa {
			rowsToKeep = append(rowsToKeep, i)
		}
	}

	for _, col := range df.Columns {
		newData := []any{}
		for _, idx := range rowsToKeep {
			newData = append(newData, col.Data[idx])
		}
		col.Data = newData
	}
}

// Astype converts the data type of a column
func (df *DataFrame) Astype(columnName string, targetType string) error {
	col, exists := df.Columns[columnName]
	if !exists {
		return fmt.Errorf("column '%s' does not exist", columnName)
	}

	newData := make([]any, len(col.Data))
	for i, v := range col.Data {
		switch targetType {
		case "int":
			if floatVal, ok := v.(float64); ok {
				newData[i] = int(floatVal)
			} else {
				return fmt.Errorf("cannot convert value '%v' to int", v)
			}
		case "float64":
			if intVal, ok := v.(int); ok {
				newData[i] = float64(intVal)
			} else {
				return fmt.Errorf("cannot convert value '%v' to float64", v)
			}
		case "string":
			newData[i] = fmt.Sprintf("%v", v)
		default:
			return fmt.Errorf("unsupported target type '%s'", targetType)
		}
	}

	col.Data = newData
	return nil
}

// Time Series Support

// AddDatetimeIndex adds a datetime index to the DataFrame
func (df *DataFrame) AddDatetimeIndex(columnName string, format string) error {
	col, exists := df.Columns[columnName]
	if !exists {
		return fmt.Errorf("column '%s' does not exist", columnName)
	}

	newData := make([]any, len(col.Data))
	for i, v := range col.Data {
		strVal, ok := v.(string)
		if !ok {
			return fmt.Errorf("value '%v' in column '%s' is not a string", v, columnName)
		}
		datetime, err := time.Parse(format, strVal)
		if err != nil {
			return fmt.Errorf("error parsing datetime '%s': %v", strVal, err)
		}
		newData[i] = datetime
	}

	col.Data = newData
	return nil
}

// Resample aggregates data based on a given time frequency
func (df *DataFrame) Resample(datetimeColumn string, freq string, aggFunc func([]any) any) (*DataFrame, error) {
	if _, exists := df.Columns[datetimeColumn]; !exists {
		return nil, fmt.Errorf("datetime column '%s' does not exist", datetimeColumn)
	}

	resampled := NewDataFrame()
	resampled.Columns[datetimeColumn] = &Column[any]{
		Name: datetimeColumn,
		Data: []any{},
	}

	for name := range df.Columns {
		if name != datetimeColumn {
			resampled.Columns[name] = &Column[any]{
				Name: name,
				Data: []any{},
			}
		}
	}

	// Group by frequency and apply aggregation
	grouped := make(map[time.Time]map[string][]any)
	for i := 0; i < df.Nrows(); i++ {
		row, _ := df.Row(i)
		datetime := row[datetimeColumn].(time.Time)
		bucket := truncateToFrequency(datetime, freq)
		if _, exists := grouped[bucket]; !exists {
			grouped[bucket] = make(map[string][]any)
		}
		for name, value := range row {
			if name != datetimeColumn {
				grouped[bucket][name] = append(grouped[bucket][name], value)
			}
		}
	}

	// Aggregate and populate the resampled DataFrame
	for bucket, data := range grouped {
		resampled.Columns[datetimeColumn].Data = append(resampled.Columns[datetimeColumn].Data, bucket)
		for name, values := range data {
			resampled.Columns[name].Data = append(resampled.Columns[name].Data, aggFunc(values))
		}
	}

	return resampled, nil
}

// Shift shifts the data in the DataFrame by a given number of periods
func (df *DataFrame) Shift(periods int) *DataFrame {
	shifted := NewDataFrame()
	for name, col := range df.Columns {
		newData := make([]any, len(col.Data))
		for i := range col.Data {
			newIdx := i - periods
			if newIdx >= 0 && newIdx < len(col.Data) {
				newData[i] = col.Data[newIdx]
			} else {
				newData[i] = nil
			}
		}
		shifted.Columns[name] = &Column[any]{
			Name: name,
			Data: newData,
		}
	}
	return shifted
}

// truncateToFrequency truncates a time to the specified frequency
func truncateToFrequency(t time.Time, freq string) time.Time {
	switch freq {
	case "Y":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "M":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "D":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "H":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case "T":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case "S":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
	default:
		return t
	}
}

// Visualization Support

// LinePlot generates a line plot for the specified columns and saves it to a file
func (df *DataFrame) LinePlot(xCol, yCol, outputFile string) error {
	xData, xExists := df.Columns[xCol]
	yData, yExists := df.Columns[yCol]
	if !xExists || !yExists {
		return fmt.Errorf("specified columns '%s' or '%s' do not exist", xCol, yCol)
	}

	xValues := make([]float64, len(xData.Data))
	yValues := make([]float64, len(yData.Data))

	for i := 0; i < len(xData.Data); i++ {
		xVal, xOk := xData.Data[i].(float64)
		yVal, yOk := yData.Data[i].(float64)
		if !xOk || !yOk {
			return fmt.Errorf("non-numeric data found in columns '%s' or '%s'", xCol, yCol)
		}
		xValues[i] = xVal
		yValues[i] = yVal
	}

	graph := chart.Chart{
		Series: []chart.Series{
			chart.ContinuousSeries{
				XValues: xValues,
				YValues: yValues,
			},
		},
	}

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer file.Close()

	return graph.Render(chart.PNG, file)
}

// BarPlot generates a bar plot for the specified column and saves it to a file
func (df *DataFrame) BarPlot(columnName, outputFile string) error {
	col, exists := df.Columns[columnName]
	if !exists {
		return fmt.Errorf("specified column '%s' does not exist", columnName)
	}

	values := make([]float64, len(col.Data))
	labels := make([]string, len(col.Data))

	for i := 0; i < len(col.Data); i++ {
		val, ok := col.Data[i].(float64)
		if !ok {
			return fmt.Errorf("non-numeric data found in column '%s'", columnName)
		}
		values[i] = val
		labels[i] = fmt.Sprintf("%v", i)
	}

	graph := chart.BarChart{
		Bars: []chart.Value{},
	}

	for i, val := range values {
		graph.Bars = append(graph.Bars, chart.Value{
			Value: val,
			Label: labels[i],
		})
	}

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer file.Close()

	return graph.Render(chart.PNG, file)
}
