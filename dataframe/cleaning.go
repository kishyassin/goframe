package dataframe

import (
	"fmt"
	"sort"
	"strings"
)

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
func (df *DataFrame) DropNa() error {
	rowsToKeep := []int{}

	for i := 0; i < df.Nrows(); i++ {
		row, err := df.Row(i)
		if err != nil {
			return fmt.Errorf("failed to select row:%v, %v", err, err)
		}
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

	return nil
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
				return fmt.Errorf("cannot convert value '%v' of type %T to int", v, v)
			}
		case "float64":
			if intVal, ok := v.(int); ok {
				newData[i] = float64(intVal)
			} else {
				return fmt.Errorf("cannot convert value '%v' of type %T to float64", v, v)
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

// DropDuplicatesOption is the parameters we can set to the DropDuplicates method.
//
// Fields:
//   - subset: The columns to identify duplicates
//   - keep: Determines which duplicates to keep, "first" keeps the first occurence,
//     "last" keeps the last occurence and "none" drops every duplicate value's row.
//   - inplace: Determines if the original DataFrame should be modified.
type DropDuplicatesOption struct {
	Subset  []string // The column names to drop duplicates
	Keep    string   //"first", "last", "none"
	Inplace bool     // To return the current modified dataframe or cloned
	// ignoreIndex bool  ---> This could be added later on, to re organise the index (Refer to pandas DropDuplicate method)
}

// DropDuplicates is the method where users can drop duplicate values in the dataframe.
// It will return a cloned DataFrame by default and only keep the FIRST occurence of the duplicate value.
//
// Parameters:
//   - options: The DropDuplicatesOption struct to optionally add parameters to this method.
//
// Returns:
//   - *DataFrame: The original dataframe with no duplicates if the inplace option in DropDuplicatesOption is true.
//   - *DataFrame (cloned): A cloned DataFrame if the inplace option in DropDuplicatesOption is false.
func (df *DataFrame) DropDuplicates(options ...DropDuplicatesOption) (*DataFrame, error) {
	var colNames []string
	finalOptions := DropDuplicatesOption{Keep: "first", Inplace: false}
	newDf := NewDataFrame()

	if len(options) > 0 {
		userOpt := options[0]

		// only overwrite Subset if user provided one
		if len(userOpt.Subset) > 0 {
			finalOptions.Subset = userOpt.Subset
		}

		// only overwrite Keep if user provided one (not empty)
		if userOpt.Keep != "" {
			finalOptions.Keep = userOpt.Keep
		}

		finalOptions.Inplace = userOpt.Inplace
	}

	if len(finalOptions.Subset) > 0 {
		// if the options has columns to specifically target
		colNames = finalOptions.Subset
	} else {
		// else just do it for all columns
		colNames = df.ColumnNames()
	}

	// find which indexes in a column to keep (which rows to keep), prob create another helper function to do this
	seen := make(map[string]bool)
	indexesToKeep := []int{}

	switch finalOptions.Keep {
	case "first":
		// logic for finalOptions.keep = "first"
		for i := 0; i < df.Nrows(); i++ {
			rowKey, err := df.getRowKey(i, colNames)
			if err != nil {
				return nil, fmt.Errorf("Error, could not get row key: %v", err)
			}
			if !seen[rowKey] {
				seen[rowKey] = true
				indexesToKeep = append(indexesToKeep, i)
			}
		}

	case "last":
		// logic for finalOptions.keep = "last"
		for i := df.Nrows() - 1; i >= 0; i-- {
			rowKey, err := df.getRowKey(i, colNames)
			if err != nil {
				return nil, fmt.Errorf("Error, could not get row key: %v", err)
			}
			if !seen[rowKey] {
				seen[rowKey] = true
				indexesToKeep = append(indexesToKeep, i)
			}
		}
		sort.Ints(indexesToKeep)

	case "none":
		// logic for finalOptions.keep = "none"

		// count all the times a specific row has occured before
		counts := make(map[string]int)
		for i := 0; i < df.Nrows(); i++ {
			key, err := df.getRowKey(i, colNames)
			if err != nil {
				return nil, fmt.Errorf("Error, could not get row key: %v", err)
			}
			counts[key]++
		}

		// only keep the indexes where the count == 1
		for i := 0; i < df.Nrows(); i++ {
			key, err := df.getRowKey(i, colNames)
			if err != nil {
				return nil, fmt.Errorf("Error, could not get row key: %v", err)
			}
			if counts[key] == 1 {
				indexesToKeep = append(indexesToKeep, i)
			}
		}

	}

	for _, colName := range df.ColumnNames() {
		// iterate through each column and get the indexes
		// then directly change it in the current dataframe
		// or add the new data to a new dataframe to return.
		newData, err := df.getSubSlice(colName, indexesToKeep)

		if err != nil {
			return nil, fmt.Errorf("Error, could not get sub slice: %v", err)
		}

		// use inplace option to check whether we want to clone or modify the current dataframe
		if finalOptions.Inplace {
			// replace the existing data with the new data
			df.Columns[colName].Data = newData
		} else {
			err := newDf.AddColumn(ConvertToAnyColumn(NewColumn(colName, newData)))
			if err != nil {
				return nil, fmt.Errorf("Error trying to add column to new DataFrame: %v", err)
			}
		}
	}

	if finalOptions.Inplace {
		return df, nil
	}

	return newDf, nil
	// return the DataFrame
}

// getRowKey is a helper function to return a unique key for a row
func (df *DataFrame) getRowKey(rowIndex int, colNames []string) (string, error) {

	var builder strings.Builder

	for _, name := range colNames {
		col, ok := df.Columns[name]
		if !ok {
			return "", fmt.Errorf("Column %s not found", name)
		}
		value := col.Data[rowIndex]

		// add the col name to prevent similar values but different column cases
		builder.WriteString(name)
		builder.WriteString(":")

		if value == nil {
			builder.WriteString("nil")
		} else {
			builder.WriteString(fmt.Sprintf("%v", value))
		}

		builder.WriteString("|")
	}

	return builder.String(), nil
}

// getSubSlice is a method get a portion of an existing column and returns a slice
func (df *DataFrame) getSubSlice(colName string, indexesToKeep []int) ([]any, error) {
	col, ok := df.Columns[colName]
	if !ok {
		return nil, fmt.Errorf("Column %s not found", colName)
	}

	finalRows := make([]any, len(indexesToKeep))

	for i, index := range indexesToKeep {
		// use direct assignment instead of appending because appending costs more
		finalRows[i] = col.Data[index]
	}

	return finalRows, nil
}
