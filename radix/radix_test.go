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
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}

	// Test finding all values with a non-existing prefix
	values = root.FindAll("1234567890")
	if len(values) != 0 {
		t.Errorf("Expected 0 values, but got %d", len(values))
	}
}
func TestGetAll(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})
	root.Insert("1526985054069-3", Data{Temperature: 28, Humidity: 53})

	expectedValues := []interface{}{
		Data{Temperature: 25, Humidity: 50},
		Data{Temperature: 26, Humidity: 51},
		Data{Temperature: 27, Humidity: 52},
		Data{Temperature: 28, Humidity: 53},
	}

	values := root.GetAll()

	if !slices.Equal(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}
}
func TestGetFirst(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})
	root.Insert("1526985054069-3", Data{Temperature: 28, Humidity: 53})
	root.Insert("1526985054070-0", Data{Temperature: 29, Humidity: 54})
	root.Insert("1526985054071-0", Data{Temperature: 30, Humidity: 55})
	root.Insert("1526985054072-0", Data{Temperature: 31, Humidity: 56})
	root.Insert("1526985054073-0", Data{Temperature: 32, Humidity: 57})

	// Test getting the first value
	key, value, ok := root.GetFirst()
	if !ok {
		t.Error("Failed to get the first value")
	}
	expectedKey := "1526985054069-0"
	if key != expectedKey {
		t.Errorf("Expected key %s, but got %s", expectedKey, key)
	}
	expectedValue := Data{Temperature: 25, Humidity: 50}
	if value != expectedValue {
		t.Errorf("Expected value %v, but got %v", expectedValue, value)
	}

	// Test getting the first value from an empty radix
	emptyRoot := NewRadix()
	_, _, ok = emptyRoot.GetFirst()
	if ok {
		t.Error("Got the first value from an empty radix")
	}
}
func TestGetLast(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})
	root.Insert("1526985054069-3", Data{Temperature: 28, Humidity: 53})
	// Test getting the last value
	key, value, ok := root.GetLast()
	if !ok {
		t.Error("Failed to get the last value")
	}
	expectedKey := "1526985054069-3"
	if key != expectedKey {
		t.Errorf("Expected key %s, but got %s", expectedKey, key)
	}
	expectedValue := Data{Temperature: 28, Humidity: 53}
	if value != expectedValue {
		t.Errorf("Expected value %v, but got %v", expectedValue, value)
	}
	// Test getting the last value from an empty radix
	emptyRoot := NewRadix()
	_, _, ok = emptyRoot.GetLast()
	if ok {
		t.Error("Got the last value from an empty radix")
	}
}
func TestGetNext(t *testing.T) {
	root := NewRadix()
	root.Insert("1526985054069-0", Data{Temperature: 25, Humidity: 50})
	root.Insert("1526985054069-1", Data{Temperature: 26, Humidity: 51})
	root.Insert("1526985054069-2", Data{Temperature: 27, Humidity: 52})
	root.Insert("1526985054069-3", Data{Temperature: 28, Humidity: 53})
	root.Insert("1526985054070-0", Data{Temperature: 29, Humidity: 54})
	root.Insert("1526985054070-1", Data{Temperature: 30, Humidity: 55})

	// Test getting the next value
	key, value, ok := root.GetNext("1526985054069-1")
	if !ok {
		t.Error("Failed to get the next value")
	}
	expectedKey := "1526985054069-2"
	if key != expectedKey {
		t.Errorf("Expected key %s, but got %s", expectedKey, key)
	}
	expectedValue := Data{Temperature: 27, Humidity: 52}
	if value != expectedValue {
		t.Errorf("Expected value %v, but got %v", expectedValue, value)
	}

	// Test getting the next value for a key with a different prefix
	key, value, ok = root.GetNext("1526985054069-3")
	if !ok {
		t.Error("Failed to get the next value")
	}
	expectedKey = "1526985054070-0"
	if key != expectedKey {
		t.Errorf("Expected key %s, but got %s", expectedKey, key)
	}
	expectedValue = Data{Temperature: 29, Humidity: 54}
	if value != expectedValue {
		t.Errorf("Expected value %v, but got %v", expectedValue, value)
	}

	// Test getting the next value for the last key
	key, value, ok = root.GetNext("1526985054070-1")
	if ok {
		t.Error("Got the next value for the last key")
	}

	// Test getting the next value for a non-existing key
	root.Insert("1526985054070-3", Data{Temperature: 31, Humidity: 56})
	root.Insert("1526985054070-4", Data{Temperature: 32, Humidity: 57})
	root.Insert("1526985054070-5", Data{Temperature: 33, Humidity: 58})
	root.Insert("1526985054070-6", Data{Temperature: 34, Humidity: 59})
	key, value, ok = root.GetNext("1526985054070-2")
	if !ok {
		t.Error("Failed to get the next value for a non-existing key")
	}
	expectedKey = "1526985054070-3"
	if key != expectedKey {
		t.Errorf("Expected key %s, but got %s", expectedKey, key)
	}
	expectedValue = Data{Temperature: 31, Humidity: 56}
	if value != expectedValue {
		t.Errorf("Expected value %v, but got %v", expectedValue, value)
	}
}
