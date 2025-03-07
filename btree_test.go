package main

import (
	"sort"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// This harness struct simulates pages in memory.
type Harness struct {
	tree  BTree
	ref   map[string]string // reference data
	pages map[uint64]BNode  // in-memory pages
}

func NewHarness() *Harness {
	pages := map[uint64]BNode{}
	return &Harness{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				assert(ok, "page not found")
				return node
			},
			alloc: func(node []byte) uint64 {
				assert(BNode(node).nBytes() <= BTREE_PAGE_SIZE, "node size cannot be allocated with more than a page of memory")
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				assert(pages[ptr] == nil, "cannot put node in already taken place")
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				assert(pages[ptr] != nil, "attempted deallocation on nil page")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *Harness) add(key string, val string) {
	err := c.tree.Insert([]byte(key), []byte(val))
	assert(err == nil, "Insert failed")
	c.ref[key] = val
}

func (c *Harness) del(key string) bool {
	delete(c.ref, key)
	deleted, err := c.tree.Delete([]byte(key))
	assert(err == nil, "Delete failed")
	return deleted
}

func (c *Harness) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := BNode(c.tree.get(ptr))
		nkeys := node.nkeys()
		if node.btype() == BNODE_LEAF {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getValue(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPointer(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(c.tree.root)
	assert(keys[0] == "", "root not dumped properly")
	assert(vals[0] == "", "root not dumped properly")
	return keys[1:], vals[1:]
}

type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (self sortIF) Len() int {
	return self.len
}
func (self sortIF) Less(i, j int) bool {
	return self.less(i, j)
}
func (self sortIF) Swap(i, j int) {
	self.swap(i, j)
}

func (c *Harness) verify(t *testing.T) {
	keys, vals := c.dump()

	rkeys, rvals := []string{}, []string{}
	for k, v := range c.ref {
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
	}
	require.Equal(t, len(rkeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rkeys),
		less: func(i, j int) bool { return rkeys[i] < rkeys[j] },
		swap: func(i, j int) {
			k, v := rkeys[i], rvals[i]
			rkeys[i], rvals[i] = rkeys[j], rvals[j]
			rkeys[j], rvals[j] = k, v
		},
	})

	require.Equal(t, rkeys, keys)
	require.Equal(t, rvals, vals)

	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys >= 1, "keys must be greater than 0")
		if node.btype() == BNODE_LEAF {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			kid := BNode(c.tree.get(node.getPointer(i)))
			require.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(c.tree.get(c.tree.root))
}
