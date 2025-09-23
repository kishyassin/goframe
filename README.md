# goframe

goframe is a Go package inspired by Python's pandas, designed for data manipulation and analysis. It provides a `DataFrame` structure and `Column` types for handling and processing structured data efficiently.

[![Go Reference](https://pkg.go.dev/badge/github.com/kishyassin/goframe.svg)](https://pkg.go.dev/github.com/kishyassin/goframe)

## Features

- Typed columns with support for `int`, `float64`, `string`, and `bool`.
- DataFrame operations such as adding/removing columns, filtering rows, and selecting subsets.
- Auto-detection of column types during CSV import.
- Statistical aggregations like `Mean`, `Sum`, `Min`, and `Max`.
- **Join operations**: Perform `inner`, `left`, `right`, and `outer` joins between DataFrames.
- **Row operations**: Access rows (`Row`), retrieve subsets (`Head`, `Tail`), append rows (`AppendRow`), and remove rows (`DropRow`).
- **Column renaming**: Rename columns using the `RenameColumn` method.
- **CSV export**: Save DataFrames to CSV files using `ToCSV` and `ToCSVWriter`.
- **Time Series Support**: Add datetime indexing, resampling, and shifting for time series data.
- **Visualization**: Generate line and bar plots directly from DataFrames.

## Installation

To install goframe, use:

```bash
go get github.com/kishyassin/goframe
```

## Usage

### Creating a DataFrame

```go
package main

import (
	"fmt"
	"github.com/kishyassin/goframe"
)

func main() {
	// Create a new DataFrame
	df := goframe.NewDataFrame()

	// Add columns to the DataFrame
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("age", []int{25, 30, 35}))

	// Print the DataFrame
	fmt.Println(df)
}
```

### Importing from CSV

```go
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/kishyassin/goframe"
)

func main() {
	reader := strings.NewReader(`name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000`)
	df, err := goframe.FromCSVReader(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(df)
}
```

### Joining DataFrames

```go
package main

import (
	"fmt"
	"log"

	"github.com/kishyassin/goframe"
)

func main() {
	df1 := goframe.NewDataFrame()
	df1.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("id", []int{1, 2, 3})))
	df1.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value1", []string{"A", "B", "C"})))

	df2 := goframe.NewDataFrame()
	df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("id", []int{2, 3, 4})))
	df2.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value2", []string{"X", "Y", "Z"})))

	// Perform an inner join
	joined, err := df1.InnerJoin(df2, "id")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(joined)
}
```

### Row Operations

```go
package main

import (
	"fmt"
	"github.com/kishyassin/goframe"
)

func main() {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("age", []int{25, 30, 35}))

	// Access a row
	row, _ := df.Row(1)
	fmt.Println("Row 1:", row)

	// Get the first two rows
	head := df.Head(2)
	fmt.Println("Head:", head)

	// Append a new row
	df.AppendRow(map[string]any{"name": "Diana", "age": 40})
	fmt.Println("After appending a row:", df)

	// Drop a row
	df.DropRow(1)
	fmt.Println("After dropping a row:", df)
}
```

### Renaming Columns

```go
package main

import (
	"fmt"
	"log"

	"github.com/kishyassin/goframe"
)

func main() {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("age", []int{25, 30, 35}))

	err := df.RenameColumn("name", "full_name")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Renamed column:", df)
}
```

### Exporting to CSV

```go
package main

import (
	"fmt"
	"log"

	"github.com/kishyassin/goframe"
)

func main() {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("age", []int{25, 30, 35})))

	err := df.ToCSV("output.csv")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("DataFrame exported to output.csv")
}
```

### Advanced Features

#### Time Series

```go
package main

import (
	"fmt"
	"time"
	"github.com/kishyassin/goframe"
)

func main() {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("date", []string{"2025-09-14", "2025-09-15", "2025-09-16"})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("value", []float64{10.5, 20.3, 30.7})))

	// Convert the "date" column from string to time.Time
	dateCol, _ := df.Select("date")
	var dateTimes []time.Time
	for _, dateStr := range dateCol.Data {
		parsedDate, err := time.Parse("2006-01-02", dateStr.(string))
		if err != nil {
			fmt.Println("Error parsing date:", err)
			return
		}
		dateTimes = append(dateTimes, parsedDate)
	}
	df.Columns["date"] = goframe.ConvertToAnyColumn(goframe.NewColumn("date", dateTimes))

	// Resample data to daily frequency
	resampled, err := df.Resample("date", "D", func(values []any) any {
		// Example aggregation function: calculate the sum
		sum := 0.0
		for _, v := range values {
			if num, ok := v.(float64); ok {
				sum += num
			}
		}
		return sum
	})
	if err != nil {
		fmt.Println("Error during resampling:", err)
		return
	}

	fmt.Println("Resampled DataFrame:", resampled)
}
```

#### Visualization

```go
package main

import (
	"fmt"
	"github.com/kishyassin/goframe"
)

func main() {
	df := goframe.NewDataFrame()
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("x", []float64{1, 2, 3})))
	df.AddColumn(goframe.ConvertToAnyColumn(goframe.NewColumn("y", []float64{2.5, 3.5, 4.5})))

	// Generate a line plot
	err := df.LinePlot("x", "y", "line_plot.png")
	if err != nil {
		fmt.Println("Error generating line plot:", err)
		return
	}

	fmt.Println("Line plot saved as 'line_plot.png'")
}
```

### API Reference

#### DataFrame Methods

- `NewDataFrame()`: Create a new DataFrame.
- `AddColumn(column *Column[any])`: Add a column to the DataFrame.
- `Row(index int)`: Retrieve a row by index.
- `InnerJoin(other *DataFrame, key string)`: Perform inner join operation.
- `OuterJoin(other *DataFrame, key string)`: Perform outer join operation.
- `LeftJoin(other *DataFrame, key string)`: Perform left join operation.
- `RightJoin(other *DataFrame, key string)`: Perform right join operation.
- `Resample(column string, frequency string)`: Resample time series data.
- `LinePlot(xCol, yCol, outputFile string)`: Generate a line plot.

#### Column Methods

- `NewColumn(name string, data []T)`: Create a new column.
- `At(index int)`: Retrieve a value by index.

## Contributing

We welcome contributions from the community! If you'd like to contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a clear description of your changes.

Please ensure your code adheres to the project's coding standards and includes tests for any new functionality.

## Contributors

💖 Thanks to all the contributors who made **GoFrame** possible 💖

<a href="https://github.com/kishyassin/goframe/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=kishyassin/goframe" />
</a>



## License

This project is licensed under the MIT License. See the LICENSE file for details.
