package main

import (
	"crypto/rand"
	"fmt"
	"sort"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// The tests in this file take quite a while to run. Be aware of that.
// Would probably be in our best interest to add some directives to only run this
// when it is asked to be run.

func TestBTreeBasic(t *testing.T) {
	testScenario := []struct {
		name   string
		hasher func(uint32) uint32
	}{
		{
			name:   "Ascending hash",
			hasher: func(h uint32) uint32 { return +h },
		},
		{
			name:   "Descending hash",
			hasher: func(h uint32) uint32 { return -h },
		},
		{
			name:   "random hash with fmix32 avalanche",
			hasher: fmix32,
		},
	}

	for _, test := range testScenario {
		t.Run(test.name, func(t *testing.T) {
			commonTest(t, test.hasher)
		})
	}
}

// basic common test that accepts a hashing func
func commonTest(t *testing.T, hasher func(uint32) uint32) {
	harness := NewHarness()
	harness.add("k", "v")
	harness.verify(t)

	// insert
	for i := 0; i < 250000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		val := fmt.Sprintf("vvv%d", hasher(uint32(-i)))
		harness.add(key, val)
		if i < 2000 {
			harness.verify(t)
		}
	}
	harness.verify(t)

	// del
	for i := 2000; i < 250000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		require.True(t, harness.del(key))
	}
	harness.verify(t)

	// overwrite
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		val := fmt.Sprintf("vvv%d", hasher(uint32(+i)))
		harness.add(key, val)
		harness.verify(t)
	}

	require.False(t, harness.del("kk"))

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		require.True(t, harness.del(key))
		harness.verify(t)
	}

	harness.add("k", "v2")
	harness.verify(t)
	harness.del("k")
	harness.verify(t)

	// the dummy empty key
	require.Equal(t, 1, len(harness.pages))
	require.Equal(t, uint16(1), BNode(harness.tree.get(harness.tree.root)).nkeys())
}
func TestBTreeRandLength(t *testing.T) {
	harness := NewHarness()
	for i := 0; i < 2000; i++ {
		klen := fmix32(uint32(2*i+0)) % BTREE_MAX_KEY_SIZE
		vlen := fmix32(uint32(2*i+1)) % BTREE_MAX_VAL_SIZE
		if klen == 0 {
			continue
		}

		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		harness.add(string(key), string(val))
		harness.verify(t)
	}
}

func TestBTreeIncLength(t *testing.T) {
	for l := 1; l < BTREE_MAX_KEY_SIZE+BTREE_MAX_VAL_SIZE; l++ {
		harness := NewHarness()

		klen := l
		if klen > BTREE_MAX_KEY_SIZE {
			klen = BTREE_MAX_KEY_SIZE
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)

		factor := BTREE_PAGE_SIZE / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			rand.Read(key)
			harness.add(string(key), string(val))
		}
		harness.verify(t)
	}
}

//TODO: everything below here, we could look at moving off to helper functions in a different file

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

func (s sortIF) Len() int {
	return s.len
}
func (s sortIF) Less(i, j int) bool {
	return s.less(i, j)
}
func (s sortIF) Swap(i, j int) {
	s.swap(i, j)
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

// Performs the final avalanche mix step of the 32-bit hash function
func fmix32(hash uint32) uint32 {
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return hash
}
