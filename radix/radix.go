package radix

type Radix struct {
	root *node
}

type node struct {
	edges      map[string]*node
	value      any
	isTerminal bool
}

func NewRadix() *Radix {
	return &Radix{&node{make(map[string]*node), nil, false}}
}

func newNode(val any) *node {
	return &node{make(map[string]*node), val, false}
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

	for label, node := range n.edges {
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
			newNode.edges[label[cpl:]] = node
			n.edges[label[:cpl]] = newNode
			delete(n.edges, label)
			return
		}
		newNode := newNode(nil)
		newNode.insert(key[cpl:], value)
		newNode.edges[label[cpl:]] = node
		n.edges[label[:cpl]] = newNode
		delete(n.edges, label)
		return
	}

	newNode := newNode(value)
	newNode.isTerminal = true
	n.edges[key] = newNode
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

func (r *Radix) Find(key string) (interface{}, bool) {
	return r.root.find(key)
}

func (n *node) find(key string) (interface{}, bool) {
	if len(key) == 0 {
		return n.value, n.isTerminal
	}

	for label, node := range n.edges {
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

	for label, node := range n.edges {
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

	for _, node := range n.edges {
		node.collect(values)
	}

}

func (n *node) delete(key, parentLabel string, parent *node) {
	if len(key) == 0 {
		n.isTerminal = false
		n.value = nil
		if len(n.edges) == 0 {
			if parent != nil {
				delete(parent.edges, parentLabel)
			}
		}
		return
	}

	for label, node := range n.edges {
		cpl := commonPrefixLen(key, label)
		if cpl == 0 {
			continue
		}
		if cpl == len(label) {
			node.delete(key[cpl:], label, n)
		}
		if len(n.edges) == 1 && !n.isTerminal {
			if parent != nil {
				for key := range n.edges {
					label = key
				}
				parent.edges[parentLabel+label] = n.edges[label]
				delete(parent.edges, parentLabel)
			}
		}
		return
	}
}
