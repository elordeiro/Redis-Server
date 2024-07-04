package queue

import (
	"reflect"
	"testing"
)

func TestEnqueue(t *testing.T) {
	q := NewQueue()

	// Test enqueueing an item to an empty queue
	q.Enqueue(1)
	if q.Len() != 1 {
		t.Errorf("Expected queue length 1, but got %d", q.Len())
	}
	if value, _ := q.Peek(); value != 1 {
		t.Errorf("Expected front item 1, but got %v", value)
	}

	// Test enqueueing multiple items
	q.Enqueue(2)
	q.Enqueue(3)
	if q.Len() != 3 {
		t.Errorf("Expected queue length 3, but got %d", q.Len())
	}
	if value, _ := q.Peek(); value != 1 {
		t.Errorf("Expected front item 1, but got %v", value)
	}

	// Test enqueueing items with different types
	q.Enqueue("hello")
	q.Enqueue(true)
	if q.Len() != 5 {
		t.Errorf("Expected queue length 5, but got %d", q.Len())
	}
	if value, _ := q.Peek(); value != 1 {
		t.Errorf("Expected front item 1, but got %v", value)
	}
}

func TestDequeue(t *testing.T) {
	q := NewQueue()

	// Test dequeuing from an empty queue
	_, err := q.Dequeue()
	if err == nil {
		t.Error("Expected error when dequeuing from an empty queue, but got nil")
	}

	// Test dequeuing from a non-empty queue
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	item, err := q.Dequeue()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if item != 1 {
		t.Errorf("Expected dequeued item 1, but got %v", item)
	}
	if q.Len() != 2 {
		t.Errorf("Expected queue length 2, but got %d", q.Len())
	}

	item, err = q.Dequeue()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if item != 2 {
		t.Errorf("Expected dequeued item 2, but got %v", item)
	}
	if q.Len() != 1 {
		t.Errorf("Expected queue length 1, but got %d", q.Len())
	}

	item, err = q.Dequeue()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if item != 3 {
		t.Errorf("Expected dequeued item 3, but got %v", item)
	}
	if q.Len() != 0 {
		t.Errorf("Expected queue length 0, but got %d", q.Len())
	}

	// Test dequeuing from an empty queue after previous dequeues
	_, err = q.Dequeue()
	if err == nil {
		t.Error("Expected error when dequeuing from an empty queue, but got nil")
	}
}

func TestPeek(t *testing.T) {
	q := NewQueue()
	// Test peeking from an empty queue
	_, err := q.Peek()
	if err == nil {
		t.Error("Expected error when peeking from an empty queue, but got nil")
	}
	// Test peeking from a non-empty queue
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	value, err := q.Peek()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if value != 1 {
		t.Errorf("Expected front item 1, but got %v", value)
	}
	if q.Len() != 3 {
		t.Errorf("Expected queue length 3, but got %d", q.Len())
	}
}

func TestLen(t *testing.T) {
	q := NewQueue()
	// Test length of an empty queue
	if q.Len() != 0 {
		t.Errorf("Expected queue length 0, but got %d", q.Len())
	}
	// Test length after enqueueing items
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	if q.Len() != 3 {
		t.Errorf("Expected queue length 3, but got %d", q.Len())
	}
	// Test length after dequeuing items
	q.Dequeue()
	if q.Len() != 2 {
		t.Errorf("Expected queue length 2, but got %d", q.Len())
	}
	q.Dequeue()
	if q.Len() != 1 {
		t.Errorf("Expected queue length 1, but got %d", q.Len())
	}
	q.Dequeue()
	if q.Len() != 0 {
		t.Errorf("Expected queue length 0, but got %d", q.Len())
	}
}

func TestClear(t *testing.T) {
	q := NewQueue()
	// Test clearing an empty queue
	q.Clear()
	if q.Len() != 0 {
		t.Errorf("Expected queue length 0 after clearing, but got %d", q.Len())
	}
	// Test clearing a non-empty queue
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	q.Clear()
	if q.Len() != 0 {
		t.Errorf("Expected queue length 0 after clearing, but got %d", q.Len())
	}
}

func TestIsEmpty(t *testing.T) {
	q := NewQueue()
	// Test IsEmpty on an empty queue
	if !q.IsEmpty() {
		t.Error("Expected IsEmpty to return true for an empty queue, but got false")
	}
	// Test IsEmpty on a non-empty queue
	q.Enqueue(1)
	if q.IsEmpty() {
		t.Error("Expected IsEmpty to return false for a non-empty queue, but got true")
	}
	q.Dequeue()
	if !q.IsEmpty() {
		t.Error("Expected IsEmpty to return true after dequeuing all items, but got false")
	}
}

func TestValues(t *testing.T) {
	q := NewQueue()
	// Test getting values from an empty queue
	values := q.Values()
	if len(values) != 0 {
		t.Errorf("Expected empty values slice, but got %v", values)
	}
	// Test getting values from a non-empty queue
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	values = q.Values()
	expectedValues := []any{1, 2, 3}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}
	// Test getting values after dequeuing items
	q.Dequeue()
	values = q.Values()
	expectedValues = []any{2, 3}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}
	q.Dequeue()
	values = q.Values()
	expectedValues = []any{3}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}
	q.Dequeue()
	values = q.Values()
	expectedValues = []any{}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("Expected values %v, but got %v", expectedValues, values)
	}
}
