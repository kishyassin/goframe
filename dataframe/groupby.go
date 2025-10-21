package goframe

import (
	"fmt"
	"strings"
)

type GroupedDataFrame struct {
	Groups   map[any][]map[string]any
	KeyOrder []any // This is to preserve the order of the data
	Key      string
	Err      error
}

// The Groupby method is a powerful method used for data aggregation, it involves a DataFrame to be split into groups
// based on one or more keys, then applying a function to each group and then combining the results during aggregation.
//
// Parameters:
//   - key(s): The key(s) to group the data by.
//
// Returns:
//   - *GroupedDataFrame: The grouped DataFrame, returns empty dataframe if error.
//   - error: An error if the data cannot be grouped.

func (df *DataFrame) Groupby(key any) *GroupedDataFrame {
	groups := make(map[any][]map[string]any) // GroupKey: { row[key] : value} where key is the column name
	var err error
	keyName := ""
	keyOrder := []any{}

	switch key := key.(type) {
	case string:
		keyName = key
		groups, keyOrder, err = groupByString(df, keyName, groups)
		if err != nil {
			return &GroupedDataFrame{Err: fmt.Errorf("unable to group by string: %v", err)}
		}

	case []string:
		groups, keyOrder, err = groupByList(df, key, groups)
		if err != nil {
			return &GroupedDataFrame{Err: fmt.Errorf("unable to group by string: %v", err)}
		}

	case Series:
		// do something
	case map[string]string:
		// do something
	case func(map[string]any) any:
		// do something
	default:
		return &GroupedDataFrame{Err: fmt.Errorf("unsupported groupby key type: %T", key)}
	}

	return &GroupedDataFrame{Groups: groups, Key: keyName, KeyOrder: keyOrder, Err: nil}
}

func groupByString(df *DataFrame, colName string, groups map[any][]map[string]any) (map[any][]map[string]any, []any, error) {
	_, exists := df.Columns[colName]
	keys := []any{}

	if !exists {
		return nil, nil, fmt.Errorf("Column '%s' does not exist", colName)
	}

	for i := 0; i < df.Nrows(); i++ {
		row, err := df.Row(i) //access each row in the dataframe
		if err != nil {
			return groups, nil, fmt.Errorf("unable to access row %v in the dataframe: %v", i, err)
		}
		groupKey := row[colName] // access the column name's value, it is called groupkey because it is the identifier of that row
		_, ok := groups[groupKey]
		if !ok {
			// if the groupkey doesnt exist in groups, means it is a new group therefore we record the order
			keys = append(keys, groupKey)
		}
		groups[groupKey] = append(groups[groupKey], row) // append the row to the map of maps
	}

	return groups, keys, nil

}

func groupByList(df *DataFrame, colNames []string, groups map[any][]map[string]any) (map[any][]map[string]any, []any, error) {
	keys := []any{}

	// Validate all columns exist
	for _, col := range colNames {
		if _, exists := df.Columns[col]; !exists {
			return nil, nil, fmt.Errorf("column '%s' does not exist", col)
		}
	}

	// Iterate over all rows
	for i := 0; i < df.Nrows(); i++ {
		row, err := df.Row(i)
		if err != nil {
			return groups, nil, fmt.Errorf("unable to access row %v in the dataframe: %v", i, err)
		}

		// Build composite key using all specified columns
		keyParts := make([]string, len(colNames))
		for j, col := range colNames {
			val, ok := row[col]
			if !ok {
				return nil, nil, fmt.Errorf("column '%s' missing in row %d", col, i)
			}
			keyParts[j] = fmt.Sprintf("%v", val)
		}
		groupKey := strings.Join(keyParts, "|") // e.g. "A|B"

		// Append in order
		if _, ok := groups[groupKey]; !ok {
			keys = append(keys, groupKey)
		}

		// Append row to group
		groups[groupKey] = append(groups[groupKey], row)
	}

	return groups, keys, nil
}

// The Sum method for the grouped data frame struct is to sum the column values by their column names
// that is provided in the arguments.
//
// Parameters:
//   - column name(s): The column(s) to sum all its stored values.
//
// Returns:
//   - *DataFrame: The grouped DataFrame, returns empty dataframe if error.
//   - error: An error if the data cannot be grouped.

func (gdf *GroupedDataFrame) Sum(colNames ...string) (*DataFrame, error) {
	if gdf.Err != nil {
		return nil, gdf.Err
	}
	resultDf := NewDataFrame()

	groupKeys := make([]any, 0, len(gdf.KeyOrder))
	sumsPerCol := make(map[string][]float64)
	if len(colNames) == 0 {
		colNames = gdf.GetAllColumnNames()
	}

	// Build the column values first
	for _, groupKey := range gdf.KeyOrder {
		rows := gdf.Groups[groupKey]
		groupKeys = append(groupKeys, groupKey)

		for _, colName := range colNames {
			sum := sumColumn(rows, colName)
			sumsPerCol[colName] = append(sumsPerCol[colName], sum)
		}

	}

	// Build GroupKey column
	groupCol := NewColumn("GroupKey", groupKeys)

	// Construct DataFrame
	_ = AddTypedColumn(resultDf, groupCol)

	for _, colName := range colNames {
		values := sumsPerCol[colName]
		newcol := NewColumn(colName, values)
		err := AddTypedColumn(resultDf, newcol)
		if err != nil {
			return nil, fmt.Errorf("Error trying to add type column: %v", err)
		}
	}

	return resultDf, gdf.Err

}

func (gdf *GroupedDataFrame) Error() error {
	return gdf.Err
}

/*
The sumColumn is a helper function to help sum the specific column, this is done to separate
code to make it more readable.
*/
func sumColumn(rows []map[string]any, colName string) float64 {
	sum := 0.0
	for _, rowData := range rows {
		val, ok := rowData[colName]
		if ok {
			switch v := val.(type) {
			case int:
				sum += float64(v)
			case float64:
				sum += v
			case float32:
				sum += float64(v)
			}
		}
	}

	return sum
}

func (gdf *GroupedDataFrame) GetAllColumnNames() []string {
	columnNames := []string{}
	seen := map[string]string{}

	for _, groupVal := range gdf.Groups {
		for _, rowValue := range groupVal {
			for key := range rowValue {
				if key == gdf.Key {
					continue
				}

				// if the column already exist in the slice
				_, exists := seen[key]
				if exists {
					continue
				}
				columnNames = append(columnNames, key)
				seen[key] = ""
			}
		}
	}
	return columnNames
}
