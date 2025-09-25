package goframe

import "reflect"

// Join combines two DataFrames based on a key column and join type (inner, left, right, outer).

func (df *DataFrame) InnerJoin(other *DataFrame, key string) (*DataFrame, error) {
	err := checkExists(df, other, key)
	if err != nil {
		return nil, err
	}

	result := NewDataFrame()
	err = appendCols(df, other, result)
	if err != nil {
		return nil, err
	}

	for i := 0; i < df.Nrows(); i++ {
		rowA, _ := df.Row(i)
		for j := 0; j < other.Nrows(); j++ {
			rowB, _ := other.Row(j)
			if rowA[key] == rowB[key] {
				mergedRow := mergeRows(rowA, rowB)
				df.AppendRow(result, mergedRow)
			}
		}
	}

	return result, nil

}

func (df *DataFrame) LeftJoin(other *DataFrame, key string) (*DataFrame, error) {
	err := checkExists(df, other, key)
	if err != nil {
		return nil, err
	}

	result := NewDataFrame()
	err = appendCols(df, other, result)
	if err != nil {
		return nil, err
	}

	for i := 0; i < df.Nrows(); i++ {
		rowA, _ := df.Row(i)
		matched := false
		for j := 0; j < other.Nrows(); j++ {
			rowB, _ := other.Row(j)
			if rowA[key] == rowB[key] {
				mergedRow := mergeRows(rowA, rowB)
				df.AppendRow(result, mergedRow)
				matched = true
			}
		}
		if !matched {
			df.AppendRow(result, rowA)
		}
	}

	return result, nil
}

func (df *DataFrame) RightJoin(other *DataFrame, key string) (*DataFrame, error) {
	err := checkExists(df, other, key)
	if err != nil {
		return nil, err
	}

	result := NewDataFrame()
	err = appendCols(df, other, result)
	if err != nil {
		return nil, err
	}

	for i := 0; i < other.Nrows(); i++ {
		rowB, _ := other.Row(i)
		matched := false
		for j := 0; j < df.Nrows(); j++ {
			rowA, _ := df.Row(j)
			if rowB[key] == rowA[key] {
				mergedRow := mergeRows(rowA, rowB)
				df.AppendRow(result, mergedRow)
				matched = true
			}
		}
		if !matched {
			df.AppendRow(result, rowB)
		}
	}

	return result, nil
}

func (df *DataFrame) OuterJoin(other *DataFrame, key string) (*DataFrame, error) {
	err := checkExists(df, other, key)
	if err != nil {
		return nil, err
	}

	result := NewDataFrame()
	err = appendCols(df, other, result)
	if err != nil {
		return nil, err
	}

	matchedRows := make(map[any]bool)
	for i := 0; i < df.Nrows(); i++ {
		rowA, _ := df.Row(i)
		matched := false
		for j := 0; j < other.Nrows(); j++ {
			rowB, _ := other.Row(j) // Ensure rowB is defined
			if reflect.DeepEqual(rowA[key], rowB[key]) {
				mergedRow := mergeRows(rowA, rowB)
				df.AppendRow(result, mergedRow)
				matchedRows[rowA[key]] = true
				matched = true
			}
		}
		if !matched {
			df.AppendRow(result, rowA)
		}

	}

	// Now append the rows that were not matched in the first for loop
	// this is to also add the other dataframe into the result
	for i := 0; i < other.Nrows(); i++ {
		rowB, _ := other.Row(i)
		if _, exists := matchedRows[rowB[key]]; !exists {
			df.AppendRow(result, rowB)
		}
	}

	return result, nil
}
