package goframe

import (
	"fmt"
	"os"

	"github.com/wcharczuk/go-chart/v2"
)

// Visualization Support

// LinePlot generates a line plot for the specified columns and saves it to a file
func (df *DataFrame) LinePlot(xCol, yCol, outputFile string) error {
	xData, xExists := df.Columns[xCol]
	yData, yExists := df.Columns[yCol]
	if !xExists || !yExists {
		return fmt.Errorf("specified columns '%s' or '%s' do not exist", xCol, yCol)
	}

	xValues := make([]float64, len(xData.Data))
	yValues := make([]float64, len(yData.Data))

	for i := 0; i < len(xData.Data); i++ {
		xVal, xOk := xData.Data[i].(float64)
		yVal, yOk := yData.Data[i].(float64)
		if !xOk || !yOk {
			return fmt.Errorf("non-numeric data found in columns '%s' or '%s'", xCol, yCol)
		}
		xValues[i] = xVal
		yValues[i] = yVal
	}

	graph := chart.Chart{
		Series: []chart.Series{
			chart.ContinuousSeries{
				XValues: xValues,
				YValues: yValues,
			},
		},
	}

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer file.Close()

	return graph.Render(chart.PNG, file)
}

// BarPlot generates a bar plot for the specified column and saves it to a file
func (df *DataFrame) BarPlot(columnName, outputFile string) error {
	col, exists := df.Columns[columnName]
	if !exists {
		return fmt.Errorf("specified column '%s' does not exist", columnName)
	}

	values := make([]float64, len(col.Data))
	labels := make([]string, len(col.Data))

	for i := 0; i < len(col.Data); i++ {
		val, ok := col.Data[i].(float64)
		if !ok {
			return fmt.Errorf("non-numeric data found in column '%s'", columnName)
		}
		values[i] = val
		labels[i] = fmt.Sprintf("%v", i)
	}

	graph := chart.BarChart{
		Bars: []chart.Value{},
	}

	for i, val := range values {
		graph.Bars = append(graph.Bars, chart.Value{
			Value: val,
			Label: labels[i],
		})
	}

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer file.Close()

	return graph.Render(chart.PNG, file)
}
