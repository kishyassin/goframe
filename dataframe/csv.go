package dataframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// FromCSV creates a DataFrame from a CSV file.
//
// Parameters:
//   - filename: The path to the CSV file.
//
// Returns:
//   - *DataFrame: The created DataFrame.
//   - error: An error if the file cannot be read.
func (df *DataFrame) FromCSV(filename string) (*DataFrame, error) {
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
