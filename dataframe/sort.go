package goframe

import (
	"fmt"
	"sort"
)

// DataFrameSorter is a helper structure to implement the sort.Interface.
// It allows us to use Go's standard library sort function on the DataFrame.
type DataFrameSorter struct {
	df        *DataFrame
	colName   string
	ascending bool
}

// Len is part of sort.Interface.
func (s DataFrameSorter) Len() int {
	return s.df.Nrows()
}

// Swap is part of sort.Interface. It swaps the elements at indices i and j
// across ALL columns to preserve row integrity.
func (s DataFrameSorter) Swap(i, j int) {
	for _, col := range s.df.Columns {
		// Swap the data in every column's slice
		col.Data[i], col.Data[j] = col.Data[j], col.Data[i]
	}
}

// Less is part of sort.Interface. It compares elements i and j in the sort column.
func (s DataFrameSorter) Less(i, j int) bool {
	col := s.df.Columns[s.colName]
	value1 := col.Data[i]
	value2 := col.Data[j]

	// try numeric comparison first (using the existing helper function)
	float1, ok1 := toFloat(value1)
	float2, ok2 := toFloat(value2)

	if ok1 && ok2 {
		if s.ascending {
			return float1 < float2
		}
		return float1 > float2
	}

	// fallback to string comparison for non-numeric types
	string1 := fmt.Sprintf("%v", value1)
	string2 := fmt.Sprintf("%v", value2)

	if s.ascending {
		return string1 < string2
	}
	return string1 > string2
}

// sort_values is a DataFrame method that sorts the columns and returns the new sorted DataFrame.
//
// Parameters:
//   - by : The column name to sort by.
//   - ascending (optional) : The order of the values to sort by.
//     True = Ascending,
//     False = Descending
//     If it is not declared by user, it will be ascending by default.
//
// Returns:
//   - *DataFrame: The sorted DataFrame, returns an empty dataframe if there is an error.
//   - error: An error if the operation fails.
func (df *DataFrame) SortValues(by string, ascending ...bool) (*DataFrame, error) {

	// default value is ascending
	isAscending := true
	if len(ascending) > 0 {
		isAscending = ascending[0]
	}

	// we create a new DataFrame to copy the data into for mutilation
	sortedDf := NewDataFrame()
	for name, col := range df.Columns {

		// create a new column
		newCol := &Column[any]{
			Name: col.Name,
			// create a brand new slice to copy the data
			Data: append([]any{}, col.Data...),
		}
		// directly assign the column to sortedDf
		sortedDf.Columns[name] = newCol
	}
	dfSorter := DataFrameSorter{
		df:        sortedDf,
		colName:   by,
		ascending: isAscending,
	}

	sort.Sort(dfSorter)

	return sortedDf, nil
}

// TODO: sort_index method
