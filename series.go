package goframe

import (
	"fmt"
	"math"
	"strconv"
)

// Series represents a single column of data with a name and type
type Series struct {
	Name string
	Data []interface{}
}

// NewSeries creates a new Series with the given name and data
func NewSeries(name string, data []interface{}) *Series {
	return &Series{
		Name: name,
		Data: data,
	}
}

// Len returns the length of the series
func (s *Series) Len() int {
	return len(s.Data)
}

// At returns the value at the given index
func (s *Series) At(index int) interface{} {
	if index < 0 || index >= len(s.Data) {
		return nil
	}
	return s.Data[index]
}

// AsFloat64 returns the series data as float64 slice, converting where possible
func (s *Series) AsFloat64() ([]float64, error) {
	result := make([]float64, len(s.Data))
	for i, v := range s.Data {
		switch val := v.(type) {
		case float64:
			result[i] = val
		case float32:
			result[i] = float64(val)
		case int:
			result[i] = float64(val)
		case int64:
			result[i] = float64(val)
		case string:
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %v to float64", val)
			}
			result[i] = f
		default:
			return nil, fmt.Errorf("cannot convert %T to float64", val)
		}
	}
	return result, nil
}

// Mean calculates the mean of numeric values in the series
func (s *Series) Mean() (float64, error) {
	nums, err := s.AsFloat64()
	if err != nil {
		return 0, err
	}
	if len(nums) == 0 {
		return 0, fmt.Errorf("empty series")
	}

	sum := 0.0
	for _, v := range nums {
		sum += v
	}
	return sum / float64(len(nums)), nil
}

// Sum calculates the sum of numeric values in the series
func (s *Series) Sum() (float64, error) {
	nums, err := s.AsFloat64()
	if err != nil {
		return 0, err
	}

	sum := 0.0
	for _, v := range nums {
		sum += v
	}
	return sum, nil
}

// Min finds the minimum value in the series
func (s *Series) Min() (float64, error) {
	nums, err := s.AsFloat64()
	if err != nil {
		return 0, err
	}
	if len(nums) == 0 {
		return 0, fmt.Errorf("empty series")
	}

	min := nums[0]
	for _, v := range nums[1:] {
		if v < min {
			min = v
		}
	}
	return min, nil
}

// Max finds the maximum value in the series
func (s *Series) Max() (float64, error) {
	nums, err := s.AsFloat64()
	if err != nil {
		return 0, err
	}
	if len(nums) == 0 {
		return 0, fmt.Errorf("empty series")
	}

	max := nums[0]
	for _, v := range nums[1:] {
		if v > max || math.IsNaN(max) {
			max = v
		}
	}
	return max, nil
}
