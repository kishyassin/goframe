package goframe

import "fmt"

type GroupedDataFrame struct {
	Groups	map[any][]map[string]any
	Key    	string
	Err		error
}

// The Groupby method is a powerful method used for data aggregation, it involves a DataFrame to be split into groups
// based on one or more keys, then applying a function to each group and then combining the results during aggregation.
//
// Parameters:
//   - key(s): The key(s) to group the data by.
//
// Returns:
//   - *DataFrame: The grouped DataFrame, returns empty dataframe if error.
//   - error: An error if the data cannot be grouped.

func (df *DataFrame) Groupby(key any) (*GroupedDataFrame) {
	groups := make(map[any][]map[string]any) // GroupKey: { row[key] : value} where key is the column name
	var err error
	keyName := ""

	switch key := key.(type) {
	case string:
		keyName = key
		groups, err = groupByString(df, keyName, groups)
		if err != nil {
			return &GroupedDataFrame{Err: fmt.Errorf("unable to group by string: %v", err)}
		}

	case []string:
		// do something
	case Series:
		// do something
	case map[string]string:
		// do something
	case func(map[string]any) any:
		// do something
	default:
		return &GroupedDataFrame{Err: fmt.Errorf("unsupported groupby key type: %T", key)}
	}

	return &GroupedDataFrame{Groups: groups, Key: keyName, Err: nil}
}

func groupByString(df *DataFrame, colName string, groups map[any][]map[string]any) (map[any][]map[string]any, error) {
	_, exists := df.Columns[colName]
	if !exists {
		return nil, fmt.Errorf("Column '%s' does not exist", colName)
	}

	for i := 0; i < df.Nrows(); i++ {
		row, err := df.Row(i) //access each row in the dataframe
		if err != nil {
			return groups, fmt.Errorf("unable to access row %v in the dataframe: %v", i, err)
		}
		groupKey := row[colName]                         // access the column name's value, it is called groupkey because it is the identifier of that row
		groups[groupKey] = append(groups[groupKey], row) // append the row to the map of maps
	}

	return groups, nil

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
	if gdf.Err != nil{
		return nil, gdf.Err
	}

	resultDf := NewDataFrame()

	for _, rows := range gdf.Groups {
		if len(colNames) > 0 {
			// user provided columns
			for _, colName := range colNames {
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
				//create the column
				newcol := NewColumn(colName, []float64{sum})

				err := AddTypedColumn(resultDf, newcol)
				if err != nil {
					return nil, fmt.Errorf("failed to add column '%s': %v", colName, err)
				}
			}

		}
	}

	return resultDf, gdf.Err

}

func (gdf *GroupedDataFrame) Error() error{
	return gdf.Err
}
