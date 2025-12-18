package dataframe

import (
	"fmt"
	"time"
)

// Time Series Support

// AddDatetimeIndex adds a datetime index to the DataFrame
func (df *DataFrame) AddDatetimeIndex(columnName string, format string) error {
	col, exists := df.Columns[columnName]
	if !exists {
		return fmt.Errorf("column '%s' does not exist", columnName)
	}

	newData := make([]any, len(col.Data))
	for i, v := range col.Data {
		strVal, ok := v.(string)
		if !ok {
			return fmt.Errorf("value '%v' in column '%s' is not a string", v, columnName)
		}
		datetime, err := time.Parse(format, strVal)
		if err != nil {
			return fmt.Errorf("error parsing datetime '%s': %v", strVal, err)
		}
		newData[i] = datetime
	}

	col.Data = newData
	return nil
}

// Resample aggregates data based on a given time frequency
func (df *DataFrame) Resample(datetimeColumn string, freq string, aggFunc func([]any) any) (*DataFrame, error) {
	if _, exists := df.Columns[datetimeColumn]; !exists {
		return nil, fmt.Errorf("datetime column '%s' does not exist", datetimeColumn)
	}

	resampled := NewDataFrame()
	resampled.Columns[datetimeColumn] = &Column[any]{
		Name: datetimeColumn,
		Data: []any{},
	}

	for name := range df.Columns {
		if name != datetimeColumn {
			resampled.Columns[name] = &Column[any]{
				Name: name,
				Data: []any{},
			}
		}
	}

	// Group by frequency and apply aggregation
	grouped := make(map[time.Time]map[string][]any)
	for i := 0; i < df.Nrows(); i++ {
		row, _ := df.Row(i)
		datetime := row[datetimeColumn].(time.Time)
		bucket := truncateToFrequency(datetime, freq)
		if _, exists := grouped[bucket]; !exists {
			grouped[bucket] = make(map[string][]any)
		}
		for name, value := range row {
			if name != datetimeColumn {
				grouped[bucket][name] = append(grouped[bucket][name], value)
			}
		}
	}

	// Aggregate and populate the resampled DataFrame
	for bucket, data := range grouped {
		resampled.Columns[datetimeColumn].Data = append(resampled.Columns[datetimeColumn].Data, bucket)
		for name, values := range data {
			resampled.Columns[name].Data = append(resampled.Columns[name].Data, aggFunc(values))
		}
	}

	return resampled, nil
}

// Shift shifts the data in the DataFrame by a given number of periods
func (df *DataFrame) Shift(periods int) *DataFrame {
	shifted := NewDataFrame()
	for name, col := range df.Columns {
		newData := make([]any, len(col.Data))
		for i := range col.Data {
			newIdx := i - periods
			if newIdx >= 0 && newIdx < len(col.Data) {
				newData[i] = col.Data[newIdx]
			} else {
				newData[i] = nil
			}
		}
		shifted.Columns[name] = &Column[any]{
			Name: name,
			Data: newData,
		}
	}
	return shifted
}

// truncateToFrequency truncates a time to the specified frequency
func truncateToFrequency(t time.Time, freq string) time.Time {
	switch freq {
	case "Y":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "M":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "D":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "H":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case "T":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case "S":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
	default:
		return t
	}
}
