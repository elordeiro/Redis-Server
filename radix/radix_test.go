package radix

import (
	"reflect"
	"slices"
	"testing"
)

type Data struct {
	Temperature int
	Humidity    int
}

func TestInsert(t *testing.T) {
	root := NewRadix()

	// Test inserting a single key-value pair
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	if _, ok := root.Find("1526985054069-0"); !ok {
		t.Error("Failed to insert key-value pair")
	}

	// Test inserting multiple key-value pairs
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})

	if _, ok := root.Find("1526985054069-1"); !ok {
		t.Error("Failed to insert key-value pair")
	}
	if _, ok := root.Find("1526985054069-2"); !ok {
		t.Error("Failed to insert key-value pair")
	}

	// Test inserting a key that already exists
	root.Insert("1526985054069-0", Data{Temperature: 28, Humidity: 53})
	if _, ok := root.Find("1526985054069-0"); !ok {
		t.Error("Failed to insert key-value pair")
	}
}
func TestFind(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})

	// Test finding an existing key
	value, ok := root.Find("1526985054069-0")
	if !ok {
		t.Error("Failed to find existing key")
	}
	expectedValue := Data{Temperature: 25, Humidity: 50}
	if value != expectedValue {
		t.Errorf("Expected value %v, but got %v", expectedValue, value)
	}

	// Test finding a non-existing key
	_, ok = root.Find("1526985054069-3")
	if ok {
		t.Error("Found non-existing key")
	}
}
func TestDelete(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})

	// Test deleting an existing key
	root.Delete("1526985054069-0")
	_, ok := root.Find("1526985054069-0")
	if ok {
		t.Error("Failed to delete existing key")
	}

	// Test deleting a non-existing key
	root.Delete("1526985054069-3")
	_, ok = root.Find("1526985054069-3")
	if ok {
		t.Error("Deleted non-existing key")
	}
}
func TestFindAll(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})
	root.Insert("1526985054069-3", Data{Temperature: 28, Humidity: 53})

	// Test finding all values with a given prefix
	values := root.FindAll("1526985054069-")
	expectedValues := []interface{}{
		Data{Temperature: 25, Humidity: 50},
		Data{Temperature: 26, Humidity: 51},
		Data{Temperature: 27, Humidity: 52},
		Data{Temperature: 28, Humidity: 53},
	}
	for _, value := range expectedValues {
		if !slices.Contains(values, value) {
			t.Errorf("Expected values %v, but got %v", expectedValues, values)
		}
	}
	if len(values) != len(expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}

	// Test finding all values with a non-existing prefix
	values = root.FindAll("1234567890")
	expectedValues = []interface{}{}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}
}
