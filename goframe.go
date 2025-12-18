// Package goframe provides a simple and flexible framework for working with tabular data in Go.
// It includes support for creating, manipulating, and analyzing data frames, as well as exporting
// and importing data from CSV files. The package is designed to be type-safe and easy to use,
// making it suitable for data analysis, machine learning, and general data processing tasks.
package goframe

import (
	"io"

	df "github.com/kishyassin/goframe/dataframe"
)

// Re-export all public types from the dataframe package
type DataFrame = df.DataFrame
type Series = df.Series
type MultiIndex = df.MultiIndex
type GroupedDataFrame = df.GroupedDataFrame
type DataFrameSorter = df.DataFrameSorter
type FuncType = df.FuncType

// Column is re-exported as a generic type alias
type Column[T any] = df.Column[T]

// Re-export all public constructor and utility functions

// NewDataFrame creates a new empty DataFrame.
func NewDataFrame() *DataFrame {
	return df.NewDataFrame()
}

// NewSeries creates a new Series with the given name and data.
func NewSeries(name string, data []any) *Series {
	return df.NewSeries(name, data)
}

// NewColumn creates a new Column with the given name and data.
func NewColumn[T any](name string, data []T) *Column[T] {
	return df.NewColumn(name, data)
}

// AddTypedColumn adds a typed column to a DataFrame.
func AddTypedColumn[T any](df_inst *DataFrame, col *Column[T]) error {
	return df.AddTypedColumn(df_inst, col)
}

// ConvertToAnyColumn converts a typed Column to a Column of any type.
func ConvertToAnyColumn[T any](col *Column[T]) *Column[any] {
	return df.ConvertToAnyColumn(col)
}

// FromCSVReader creates a DataFrame from a CSV reader.
func FromCSVReader(reader io.Reader) (*DataFrame, error) {
	return df.FromCSVReader(reader)
}
