# goframe

goframe is a Go package inspired by Python's pandas, designed for data manipulation and analysis. It provides a `DataFrame` structure and `Column` types for handling and processing structured data efficiently.

## Features

- Typed columns with support for `int`, `float64`, `string`, and `bool`.
- DataFrame operations such as adding/removing columns, filtering rows, and selecting subsets.
- Auto-detection of column types during CSV import.
- Statistical aggregations like `Mean`, `Sum`, `Min`, and `Max`.

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
	df := goframe.NewDataFrame()

	// Add columns
	nameCol := goframe.NewColumn("name", []string{"Alice", "Bob", "Charlie"})
	df.AddColumn(nameCol)

	ageCol := goframe.NewColumn("age", []int{25, 30, 35})
	df.AddColumn(ageCol)

	fmt.Println(df)
}
```

### Importing from CSV

```go
reader := strings.NewReader(`name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000`)
df, err := goframe.FromCSVReader(reader)
if err != nil {
	log.Fatal(err)
}
fmt.Println(df)
```

## Contributing

We welcome contributions from the community! If you'd like to contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a clear description of your changes.

Please ensure your code adheres to the project's coding standards and includes tests for any new functionality.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
