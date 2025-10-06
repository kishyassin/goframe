package goframe

/*

	This is where Aggregation methods for the DataFrame struct are defined

*/

import "fmt"

// Mean calculates the mean of numeric values for each column in the DataFrame
func (df *DataFrame) Mean() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		mean, err := series.Mean()
		if err != nil {
			return nil, fmt.Errorf("error calculating mean for column '%s': %w", name, err)
		}
		results[name] = mean
	}
	return results, nil
}

// Sum calculates the sum of numeric values for each column in the DataFrame
func (df *DataFrame) Sum() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		sum, err := series.Sum()
		if err != nil {
			return nil, fmt.Errorf("error calculating sum for column '%s': %w", name, err)
		}
		results[name] = sum
	}
	return results, nil
}

// Min calculates the minimum value for each column in the DataFrame
func (df *DataFrame) Min() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		min, err := series.Min()
		if err != nil {
			return nil, fmt.Errorf("error calculating min for column '%s': %w", name, err)
		}
		results[name] = min
	}
	return results, nil
}

// Max calculates the maximum value for each column in the DataFrame
func (df *DataFrame) Max() (map[string]float64, error) {
	results := make(map[string]float64)
	for name, col := range df.Columns {
		series := &Series{Name: name, Data: col.Data}
		max, err := series.Max()
		if err != nil {
			return nil, fmt.Errorf("error calculating max for column '%s': %w", name, err)
		}
		results[name] = max
	}
	return results, nil
}
