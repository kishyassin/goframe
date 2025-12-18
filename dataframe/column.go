package dataframe

/*

	This is where column structs and methods are defined

*/

import (
	"fmt"
)

// Column represents a typed column in the DataFrame
// T is the type of the column data (e.g., int, float64, string, bool)
type Column[T any] struct {
	Name string
	Data []T
}

// AddTypedColumn adds a typed column to the DataFrame.
//
// Parameters:
//   - df: The DataFrame to which the column will be added.
//   - col: The typed column to add.
//
// Returns:
//   - error: An error if the operation fails.
func AddTypedColumn[T any](df *DataFrame, col *Column[T]) error {
	// Automatically convert the column to *Column[any]
	anyCol := ConvertToAnyColumn(col)
	return df.AddColumn(anyCol)
}

// Len returns the length of the column
func (c *Column[T]) Len() int {
	return len(c.Data)
}

// At returns the value at the given index
func (c *Column[T]) At(index int) (T, error) {
	if index < 0 || index >= len(c.Data) {
		var zero T
		return zero, fmt.Errorf("index out of bounds")
	}
	return c.Data[index], nil
}

// ConvertToAnyColumn converts a typed column to a generic column of type `any`
func ConvertToAnyColumn[T any](col *Column[T]) *Column[any] {
	genericData := make([]any, len(col.Data))
	for i, v := range col.Data {
		genericData[i] = v
	}
	return &Column[any]{
		Name: col.Name,
		Data: genericData,
	}
}
