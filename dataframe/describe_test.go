package dataframe

import "testing"

func TestDescribe(t *testing.T) {
	df := NewDataFrame()

	df.AddColumn(ConvertToAnyColumn(NewColumn("age", []int{20, 30, 40})))
	df.AddColumn(ConvertToAnyColumn(NewColumn("salary", []float64{1000, 2000, 3000})))
	df.AddColumn(ConvertToAnyColumn(NewColumn("name", []string{"a", "b", "c"})))

	desc := df.Describe()

	if desc.Ncols() != 3 {
		t.Fatalf("expected 3 columns (stat, age, salary), got %d", desc.Ncols())
	}

	meanAge, _ := desc.Columns["age"].At(1)
	if meanAge.(float64) != 30 {
		t.Fatalf("expected mean age 30, got %v", meanAge)
	}
}
