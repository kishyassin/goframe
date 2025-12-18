package dataframe

import "fmt"

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
