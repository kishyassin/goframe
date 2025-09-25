package goframe

import "fmt"

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
