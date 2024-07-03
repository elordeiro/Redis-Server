package radix

type Radix struct {
	root *node
}

type node struct {
	edges      []edge
	value      any
	isTerminal bool
}

type edge struct {
	label string
	node  *node
}

func NewRadix() *Radix {
	return &Radix{&node{[]edge{}, nil, false}}
}

func newNode(val any) *node {
	return &node{[]edge{}, val, false}
}

func newEdge(label string, node *node) edge {
	return edge{label, node}
}

func (r *Radix) Insert(key string, value interface{}) {
	r.root.insert(key, value)
}

func (n *node) insert(key string, value interface{}) {
	if len(key) == 0 {
		n.isTerminal = true
		n.value = value
		return
	}

	for _, edge := range n.edges {
		label := edge.label
		node := edge.node
		cpl := commonPrefixLen(key, label)
		if cpl == 0 {
			continue
		}
		if cpl == len(label) {
			node.insert(key[cpl:], value)
			return
		}
		if cpl == len(key) {
			newNode := newNode(value)
			newNode.isTerminal = true
			newNode.edges = append(newNode.edges, newEdge(label[cpl:], node))
			n.updateEdge(label, label[:cpl], newNode)
			n.deleteEdge(label)
			return
		}
		newNode := newNode(nil)
		newNode.edges = append(newNode.edges, newEdge(label[cpl:], node))
		newNode.insert(key[cpl:], value)
		n.updateEdge(label, label[:cpl], newNode)
		n.deleteEdge(label)
		return
	}

	newNode := newNode(value)
	newNode.isTerminal = true
	n.edges = append(n.edges, newEdge(key, newNode))
}

func commonPrefixLen(a, b string) int {
	minLen := min(len(a), len(b))
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return minLen
}

func (n *node) updateEdge(oldLabel, newLabel string, node *node) {
	for i, edge := range n.edges {
		if edge.label == oldLabel {
			n.edges[i].label = newLabel
			n.edges[i].node = node
			return
		}
	}
}

func (n *node) deleteEdge(label string) {
	for i, edge := range n.edges {
		if edge.label == label {
			n.edges = append(n.edges[:i], n.edges[i+1:]...)
			return
		}
	}
}

func (r *Radix) Find(key string) (interface{}, bool) {
	return r.root.find(key)
}

func (n *node) find(key string) (interface{}, bool) {
	if len(key) == 0 {
		return n.value, n.isTerminal
	}

	for _, edge := range n.edges {
		label := edge.label
		node := edge.node
		cpl := commonPrefixLen(key, label)
		if cpl == 0 {
			continue
		}
		if cpl == len(label) {
			return node.find(key[cpl:])
		}
		if cpl == len(key) {
			return nil, false
		}
		return nil, false
	}

	return nil, false
}

func (r *Radix) Delete(key string) {
	r.root.delete(key, "", nil)
}

func (r *Radix) FindAll(prefix string) []interface{} {
	values := []interface{}{}
	r.root.findAll(prefix, &values)
	return values
}

func (n *node) findAll(prefix string, values *[]interface{}) {
	if n.isTerminal {
		*values = append(*values, n.value)
	}

	for _, edge := range n.edges {
		label := edge.label
		node := edge.node
		clp := commonPrefixLen(prefix, label)
		if clp == 0 {
			continue
		}
		if clp == len(label) {
			node.findAll(prefix[clp:], values)
		}
		if clp == len(prefix) {
			node.collect(values)
		}
	}
}

func (n *node) collect(values *[]interface{}) {
	if n.isTerminal {
		*values = append(*values, n.value)
	}

	for _, edge := range n.edges {
		edge.node.collect(values)
	}
}

func (r *Radix) GetAll() []interface{} {
	values := []interface{}{}
	r.root.collect(&values)
	return values
}

func (r *Radix) GetFirst() (string, interface{}, bool) {
	return r.root.getFirst("")
}

func (n *node) getFirst(key string) (string, interface{}, bool) {
	if n.isTerminal {
		return key, n.value, true
	}

	for _, edge := range n.edges {
		label := edge.label
		node := edge.node
		k, v, ok := node.getFirst(key + label)
		if ok {
			return k, v, ok
		}
	}

	return "", nil, false
}

func (r *Radix) GetLast() (string, interface{}, bool) {
	return r.root.getLast("")
}

func (n *node) getLast(key string) (string, interface{}, bool) {
	if n.isTerminal {
		return key, n.value, true
	}

	for i := len(n.edges) - 1; i >= 0; i-- {
		edge := n.edges[i]
		label := edge.label
		node := edge.node
		k, v, ok := node.getLast(key + label)
		if ok {
			return k, v, ok
		}
	}

	return "", nil, false
}

func (n *node) delete(key, parentLabel string, parent *node) {
	if len(key) == 0 {
		n.isTerminal = false
		n.value = nil
		if len(n.edges) == 0 {
			if parent != nil {
				parent.deleteEdge(parentLabel)
			}
		}
		return
	}

	for _, edge := range n.edges {
		label := edge.label
		node := edge.node
		cpl := commonPrefixLen(key, label)
		if cpl == 0 {
			continue
		}
		if cpl == len(label) {
			node.delete(key[cpl:], label, n)
		}
		if len(n.edges) == 1 && !n.isTerminal {
			if parent != nil {
				parent.updateEdge(parentLabel, parentLabel+label, n.edges[0].node)
				parent.deleteEdge(parentLabel)
			}
		}
		return
	}
}
