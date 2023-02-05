package tree

import (
	"testing"
)

func TestIterator(t *testing.T) {
	rbt := New[string, []byte]()
	t.Run("Test iterator", func(t *testing.T) {
		it := rbt.Iterator()
		for it.Next() {
			t.Log(it.n.key, it.n.value)
		}
	})
}
