package tree

import (
	"errors"

	"golang.org/x/exp/constraints"
)

type color bool

const (
	start int = iota
	it
	end
)

const red color = true
const black color = false

type RedBlackTree[K constraints.Ordered, V any] struct {
	root *Node[K, V]
	size int
}

func (t *RedBlackTree[K, V]) Size() int {
	return t.size
}

func (t *RedBlackTree[K, V]) Iterator() Iterator[K, V] {
	return Iterator[K, V]{t: t, n: nil, p: start}
}

func (t *RedBlackTree[K, V]) Max() *Node[K, V] {
	c := t.root
	for c != nil {
		if c.right == nil {
			return c
		}
		c = c.right
	}
	return nil
}

func (t *RedBlackTree[K, V]) Min() *Node[K, V] {
	c := t.root
	for c != nil {
		if c.left == nil {
			return c
		}
		c = c.left
	}
	return nil
}

func (t *RedBlackTree[K, V]) GetNode(k K) *Node[K, V] {
	c := t.root
	for c != nil {
		if k == c.key {
			return c
		} else if k < c.key {
			c = c.left
		} else {
			c = c.right
		}
	}
	return nil
}

func (t *RedBlackTree[K, V]) Floor(k K) (*Node[K, V], error) {
	v := t.Iterator()
	if !v.Next() {
		return nil, errors.New("no floor found")
	}
	c := v.n
	for v.HasNext() {
		if !v.Next() {
			return c, nil
		}
		if v.n.key > k {
			return c, nil
		}
		c = v.n
	}
	return c, nil
}

func (t *RedBlackTree[K, V]) Get(k K) (V, error) {
	n := t.GetNode(k)
	if n != nil {
		return n.value, nil
	}
	return *new(V), errors.New("key not found")
}

func (t *RedBlackTree[K, V]) Put(k K, v V) {
	var insertedNode *Node[K, V]
	if t.root == nil {
		t.root = &Node[K, V]{key: k, value: v, c: red}
		insertedNode = t.root
	} else {
		cur := t.root
		for {
			if k == cur.key {
				cur.key = k
				cur.value = v
				return
			} else if k < cur.key {
				if cur.left == nil {
					cur.left = &Node[K, V]{key: k, value: v, c: red}
					insertedNode = cur.left
					break
				} else {
					cur = cur.left
				}
			} else {
				if cur.right == nil {
					cur.right = &Node[K, V]{key: k, value: v, c: red}
					insertedNode = cur.right
					break
				} else {
					cur = cur.right
				}
			}
		}
		insertedNode.parent = cur
	}
	t.size++
	t.insert1(insertedNode)
}

func (t *RedBlackTree[K, V]) insert1(n *Node[K, V]) {
	if n.parent == nil {
		n.c = black // insert case 1
	} else if n.parent == nil || n.parent.c == black {
		return // insert case 2
	} else if uncle := n.uncle(); uncle != nil && uncle.c == red {
		n.parent.c = black // insert case 3
		uncle.c = black
		n.parent.parent.c = red
		t.insert1(n.parent.parent)
	} else {
		if n == n.parent.right && n.parent == n.parent.parent.left {
			t.rotateLeft(n.parent) // insert case 4a
			n = n.left
		} else if n == n.parent.left && n.parent == n.parent.parent.right {
			t.rotateRight(n.parent) // insert case 4b
			n = n.right
		}
		n.parent.c = black
		n.parent.parent.c = red
		if n == n.parent.left && n.parent == n.parent.parent.left {
			t.rotateRight(n.parent.parent)
		} else if n == n.parent.right && n.parent == n.parent.parent.right {
			t.rotateLeft(n.parent.parent)
		}
	}
}

func (t *RedBlackTree[K, V]) rotateRight(n *Node[K, V]) {
	g := n.parent
	s := n.left
	c := s.right
	n.left = c
	if c != nil {
		c.parent = n
	}
	s.right = n
	n.parent = s
	s.parent = g
	if g != nil && n == g.right {
		g.right = s
	} else if g != nil && n != g.right {
		g.left = s
	} else {
		t.root = s
	}
}

func (t *RedBlackTree[K, V]) rotateLeft(n *Node[K, V]) {
	g := n.parent
	s := n.right
	c := s.left
	n.right = c
	if c != nil {
		c.parent = n
	}
	s.left = n
	n.parent = s
	s.parent = g
	if g != nil && n == g.right {
		g.right = s
	} else if g != nil && n != g.right {
		g.left = s
	} else {
		t.root = s
	}
}

func New[K constraints.Ordered, V any]() *RedBlackTree[K, V] {
	return &RedBlackTree[K, V]{root: nil, size: 0}
}

func (n *Node[K, V]) uncle() *Node[K, V] {
	if n.parent == nil || n.parent.parent == nil {
		return nil
	}
	gp := n.parent.parent
	if gp.left == n.parent {
		return gp.right
	} else {
		return gp.left
	}
}

type Node[K constraints.Ordered, V any] struct {
	key    K
	value  V
	c      color
	left   *Node[K, V]
	right  *Node[K, V]
	parent *Node[K, V]
}

func (n *Node[K, V]) Value() V {
	return n.value
}

func (n *Node[K, V]) Key() K {
	return n.key
}

type Iterator[K constraints.Ordered, V any] struct {
	t *RedBlackTree[K, V]
	n *Node[K, V]
	p int
}

func (i *Iterator[K, V]) Next() bool {
	if i.p == end {
		return i.end()
	}
	if i.p == start {
		left := i.t.Min()
		if left == nil {
			return i.end()
		}
		i.n = left
		return i.it()
	}
	if i.n.right != nil {
		i.n = i.n.right
		for i.n.left != nil {
			i.n = i.n.left
		}
		return i.it()
	}
	for i.n.parent != nil {
		node := i.n
		i.n = i.n.parent
		if node == i.n.left {
			return i.it()
		}
	}
	return i.end()
}
func (i *Iterator[K, V]) Prev() bool {
	if i.p == start {
		return i.start()
	}
	if i.p == end {
		right := i.t.Max()
		if right == nil {
			return i.start()
		}
		i.n = right
		return i.it()
	}
	if i.n.left != nil {
		i.n = i.n.left
		for i.n.right != nil {
			i.n = i.n.right
		}
		return i.it()
	}
	for i.n.parent != nil {
		n := i.n
		i.n = i.n.parent
		if n == i.n.right {
			return i.it()
		}
	}
	return i.start()
}

func (i *Iterator[K, V]) Node() *Node[K, V] {
	return i.n
}

func (i *Iterator[K, V]) HasNext() bool {
	return i.p != end
}

func (i *Iterator[K, V]) Key() K {
	return i.n.key
}

func (i *Iterator[K, V]) Value() V {
	return i.n.value
}

func (i *Iterator[K, V]) start() bool {
	i.n = nil
	i.p = start
	return false
}

func (i *Iterator[K, V]) it() bool {
	i.p = it
	return true
}

func (i *Iterator[K, V]) end() bool {
	i.n = nil
	i.p = end
	return false
}
