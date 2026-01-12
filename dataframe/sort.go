package dataframe

import (
	"fmt"
	"sort"
)

// DataFrameSorter is a helper structure to implement the sort.Interface.
// It allows us to use Go's standard library sort function on the DataFrame.
type DataFrameSorter struct {
	df        *DataFrame
	colName   []string
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
// returning false means that the order is incorrect and should be swapped.
// Example: if i is less than j, it return true for ascending
func (s DataFrameSorter) Less(i, j int) bool {

	for _, colName := range s.colName {

		col := s.df.Columns[colName]
		value1 := col.Data[i]
		value2 := col.Data[j]

		// check if they are nil value
		if value1 == nil && value2 == nil {
			continue // They are equal, move to the next column tie-breaker
		}
		if value1 == nil {
			// returning false means they are in the wrong order and should be swapped.
			return false // value1 is "greater" a row lower than value2
		}
		if value2 == nil {
			// returning true means they are in the right order and should stay that way.
			return true // value1 is "less" (comes first) than value2
		}

		// try numeric comparison first (using the existing helper function)
		float1, ok1 := toFloat(value1)
		float2, ok2 := toFloat(value2)

		if ok1 && ok2 {
			// check if they are equal, and if they are, continue to next col
			if float1 == float2 {
				continue
			}
			if s.ascending {
				return float1 < float2
			}
			return float1 > float2
		}

		// fallback to string comparison for non-numeric types
		string1 := fmt.Sprintf("%v", value1)
		string2 := fmt.Sprintf("%v", value2)
		if string1 == string2 {
			continue
		}
		if s.ascending {
			return string1 < string2
		}
		return string1 > string2
	}
	// if everything is identical, return false
	return false

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
func (df *DataFrame) SortValues(by []string, ascending ...bool) (*DataFrame, error) {

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
