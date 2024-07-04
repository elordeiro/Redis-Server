package queue

import "errors"

// Queue is a simple FIFO queue based on a circular list that resizes as needed.
type Queue struct {
	head  *node
	tail  *node
	count int
}

type node struct {
	value any
	next  *node
}

// NewQueue creates a new queue.
func NewQueue() *Queue {
	return &Queue{}
}

// newNode creates a new node
func newNode(val any) *node {
	return &node{
		value: val,
	}
}

// Enqueue adds an item to the end of the queue.
func (q *Queue) Enqueue(item any) {
	if q.count == 0 {
		nn := newNode(item)
		q.head = nn
		q.tail = nn
		q.count = 1
		return
	}
	nn := newNode(item)
	last := q.tail
	last.next = nn
	q.tail = nn
	q.count++
}

// Dequeue removes and returns the item at the front of the queue.
func (q *Queue) Dequeue() (any, error) {
	if q.count == 0 {
		return nil, errors.New("queue is empty")
	}
	first := q.head
	q.head = first.next
	q.count--
	return first.value, nil
}

// Peek returns the item at the front of the queue without removing it.
func (q *Queue) Peek() (any, error) {
	if q.count == 0 {
		return nil, errors.New("queue is empty")
	}
	return q.head.value, nil
}

// Len returns the number of items in the queue.
func (q *Queue) Len() int {
	return q.count
}

// Clear removes all items from the queue.
func (q *Queue) Clear() {
	q.head = nil
	q.tail = nil
	q.count = 0
}

// IsEmpty returns true if the queue is empty.
func (q *Queue) IsEmpty() bool {
	return q.count == 0
}

// Values returns all items in the queue.
func (q *Queue) Values() []any {
	values := []any{}
	curr := q.head
	for curr != nil {
		values = append(values, curr.value)
		curr = curr.next
	}
	return values
}
