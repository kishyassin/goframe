package dataframe

import "testing"

func TestDescribe(t *testing.T) {
	df := NewDataFrame()

	df.AddColumn(ConvertToAnyColumn(NewColumn("age", []int{20, 30, 40})))
	df.AddColumn(ConvertToAnyColumn(NewColumn("salary", []float64{1000, 2000, 3000})))
	df.AddColumn(ConvertToAnyColumn(NewColumn("name", []string{"a", "b", "c"}))) // non-numeric

	desc, err := df.Describe()
	if err != nil {
		t.Errorf("Describe returned error: %v", err)
	}

	// Should only summarize numeric columns
	if desc.Ncols() != 3 { // stat, age, salary
		t.Errorf("expected 3 columns (stat, age, salary), got %d", desc.Ncols())
	}

	// -------- AGE COLUMN TESTS --------
	countAge, _ := desc.Columns["age"].At(0)
	meanAge, _ := desc.Columns["age"].At(1)
	minAge, _ := desc.Columns["age"].At(2)
	maxAge, _ := desc.Columns["age"].At(3)

	if countAge.(float64) != 3 {
		t.Errorf("expected age count 3, got %v", countAge)
	}
	if meanAge.(float64) != 30 {
		t.Errorf("expected age mean 30, got %v", meanAge)
	}
	if minAge.(float64) != 20 {
		t.Errorf("expected age min 20, got %v", minAge)
	}
	if maxAge.(float64) != 40 {
		t.Errorf("expected age max 40, got %v", maxAge)
	}

	// -------- SALARY COLUMN TESTS --------
	countSalary, _ := desc.Columns["salary"].At(0)
	meanSalary, _ := desc.Columns["salary"].At(1)
	minSalary, _ := desc.Columns["salary"].At(2)
	maxSalary, _ := desc.Columns["salary"].At(3)

	if countSalary.(float64) != 3 {
		t.Errorf("expected salary count 3, got %v", countSalary)
	}
	if meanSalary.(float64) != 2000 {
		t.Errorf("expected salary mean 2000, got %v", meanSalary)
	}
	if minSalary.(float64) != 1000 {
		t.Errorf("expected salary min 1000, got %v", minSalary)
	}
	if maxSalary.(float64) != 3000 {
		t.Errorf("expected salary max 3000, got %v", maxSalary)
	}
}