package goframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// DataFrame represents a tabular data structure with named columns
type DataFrame struct {
	Columns []*Series
	Index   []interface{}
}

// NewDataFrame creates a new empty DataFrame
func NewDataFrame() *DataFrame {
	return &DataFrame{
		Columns: make([]*Series, 0),
		Index:   make([]interface{}, 0),
	}
}

// AddColumn adds a new column (Series) to the DataFrame
func (df *DataFrame) AddColumn(series *Series) error {
	if len(df.Columns) > 0 && series.Len() != df.Nrows() {
		return fmt.Errorf("series length %d does not match DataFrame length %d", series.Len(), df.Nrows())
	}
	df.Columns = append(df.Columns, series)

	// Initialize index if this is the first column
	if len(df.Index) == 0 {
		for i := 0; i < series.Len(); i++ {
			df.Index = append(df.Index, i)
		}
	}

	return nil
}

// Nrows returns the number of rows in the DataFrame
func (df *DataFrame) Nrows() int {
	if len(df.Columns) == 0 {
		return 0
	}
	return df.Columns[0].Len()
}

// Ncols returns the number of columns in the DataFrame
func (df *DataFrame) Ncols() int {
	return len(df.Columns)
}

// ColumnNames returns the names of all columns
func (df *DataFrame) ColumnNames() []string {
	names := make([]string, len(df.Columns))
	for i, col := range df.Columns {
		names[i] = col.Name
	}
	return names
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(columnName string) (*Series, error) {
	for _, col := range df.Columns {
		if col.Name == columnName {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column '%s' not found", columnName)
}

// Filter returns a new DataFrame with rows that satisfy the given condition
func (df *DataFrame) Filter(condition func(row []interface{}) bool) *DataFrame {
	result := NewDataFrame()

	// Create new columns with the same names
	for _, col := range df.Columns {
		newSeries := NewSeries(col.Name, []interface{}{})
		result.Columns = append(result.Columns, newSeries)
	}

	// Apply filter condition
	for i := 0; i < df.Nrows(); i++ {
		row := make([]interface{}, df.Ncols())
		for j, col := range df.Columns {
			row[j] = col.At(i)
		}

		if condition(row) {
			for j, col := range df.Columns {
				result.Columns[j].Data = append(result.Columns[j].Data, col.At(i))
			}
			result.Index = append(result.Index, df.Index[i])
		}
	}

	return result
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
		series := NewSeries(colName, []interface{}{})
		df.Columns = append(df.Columns, series)
	}

	// Read data rows
	rowIndex := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading row: %w", err)
		}

		if len(record) != len(header) {
			return nil, fmt.Errorf("row %d has %d columns, expected %d", rowIndex, len(record), len(header))
		}

		// Add data to each column, trying to parse as number if possible
		for i, value := range record {
			// Try to parse as float64 first
			if floatVal, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
				df.Columns[i].Data = append(df.Columns[i].Data, floatVal)
			} else {
				// Keep as string if parsing fails
				df.Columns[i].Data = append(df.Columns[i].Data, strings.TrimSpace(value))
			}
		}

		df.Index = append(df.Index, rowIndex)
		rowIndex++
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

	// Write data rows
	for i := 0; i < df.Nrows(); i++ {
		row := make([]string, df.Ncols())
		for j, col := range df.Columns {
			value := col.At(i)
			row[j] = fmt.Sprintf("%v", value)
		}

		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("error writing row %d: %w", i, err)
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
	for i, name := range df.ColumnNames() {
		if i > 0 {
			result.WriteString("\t")
		}
		result.WriteString(name)
	}
	result.WriteString("\n")

	// First few rows (max 10)
	maxRows := 10
	if df.Nrows() < maxRows {
		maxRows = df.Nrows()
	}

	for i := 0; i < maxRows; i++ {
		for j, col := range df.Columns {
			if j > 0 {
				result.WriteString("\t")
			}
			result.WriteString(fmt.Sprintf("%v", col.At(i)))
		}
		result.WriteString("\n")
	}

	if df.Nrows() > maxRows {
		result.WriteString("...\n")
	}

	return result.String()
}
