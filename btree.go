package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

//TODO: I think there is a chance to create a clean interface for this tree. What methods do we need to expose? What things do we need to add?

const HEADER = 4

// intentionally keeping KV size lower than a page size
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	nodeOneMax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(nodeOneMax <= BTREE_PAGE_SIZE, "node size exceeds max") // ensure under max KV
}

// Encoded as:
// | type | nkeys |  pointers  |   offsets  | key-values | unused |
// |  2B  |  2B   | nkeys * 8B | nkeys * 2B |    ...     |        |
// With KV pairs looking like:
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |

type BNode []byte // can be dumped to disk directly

type BTree struct {
	// pointer to page number
	root uint64
	// callbacks for managing on-disk pages
	get   func(uint64) []byte // dereference a pointer and reads a page from disk
	alloc func([]byte) uint64 // allocate new page (copy-on-write)
	del   func(uint64)        // deallocate a page
}

// Interface for inserting and deleting KVs
func (tree *BTree) Insert(key, value []byte) error {
	if err := checkLimit(key, value); err != nil {
		return err // the only way for an update to fail
	}

	if tree.root == 0 { // first node not created
		// create first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// we need a dummy key to cover the whole key space
		// so that a lookup will always find containing node
		// this concept is called the sentinel key
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, value)
		tree.root = tree.alloc(root) // actually allocate
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, value)
	numSplits, split := nodeSplit(node)
	tree.del(tree.root) // deallocate old root
	if numSplits > 1 {  // root was split, need a new level
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, numSplits)
		// allocate the new splits and append KVs
		for i, node := range split[:numSplits] {
			pointer, key := tree.alloc(node), node.getKey(0)
			nodeAppendKV(root, uint16(i), pointer, key, nil)
		}
		tree.root = tree.alloc(root) // allocate root
	} else {
		tree.root = tree.alloc(split[0]) // allocate root
	}

	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	if err := checkLimit(key, nil); err != nil {
		return false, err // the only way for an update to fail
	}

	if tree.root == 0 {
		return false, nil
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
		return false, nil // not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level
		tree.root = updated.getPointer(0)
	} else {
		tree.root = tree.alloc(updated)
	}
	return true, nil
}

// TODO: make these errors top level
func checkLimit(key []byte, val []byte) error {
	if len(key) == 0 {
		return errors.New("empty key") // used as a dummy key
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		return errors.New("key too long")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return errors.New("value too long")
	}
	return nil
}

// Encode/Decode helpers
// Header
const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes containing values
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// Pointers
func (node BNode) getPointer(idx uint16) uint64 {
	assert(idx < node.nkeys(), fmt.Sprintf("index of node out of range: %d > %d", idx, node.nkeys()))
	pos := HEADER + 8*idx // calc position by offset
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPointer(idx uint16, val uint64) {
	assert(idx < node.nkeys(), fmt.Sprintf("index of node out of range: %d > %d", idx, node.nkeys()))
	assert(node.btype() == BNODE_LEAF || val != 0, "value empty for leaf node")
	assert(node.btype() == BNODE_NODE || val == 0, "value non-empty for internal node")
	pos := HEADER + 8*idx // calc position by offset
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset list to find KVs - enables O(N) lookup time for KVs

// offsetPos is a helper to find the nth KV pair (as defined by the index) in our offset list.
func offsetPos(node BNode, idx uint16) uint16 {
	// NOTE: first KV is at 0 so we use the end offset for that which is the start offset for next KV
	assert(1 <= idx && idx <= node.nkeys(), "index out of range")
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

// KVs
// kvPos gives the position of a KV based on index relative to whole node.
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys(), "index out of bounds")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// getKey gives the byte value of the key at the given index.
func (node BNode) getKey(idx uint16) []byte {
	assert(idx <= node.nkeys(), "index out of bounds")
	pos := node.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:kLen]
}

// getValue gives the byte value of the value at the given index.
func (node BNode) getValue(idx uint16) []byte {
	assert(idx <= node.nkeys(), "index out of bounds")
	pos := node.kvPos(idx)                             // position in node
	kLen := binary.LittleEndian.Uint16(node[pos:])     // key length
	valLen := binary.LittleEndian.Uint16(node[pos+2:]) // val length
	return node[pos+4+kLen:][:valLen]                  // find KV at position offset and then within KV sorted list
}

// nBytes returns the size of the node in bytes. This makes use of calculating the position of the last KV due to the off-by-one
// nature of the KV offset list.
func (node BNode) nBytes() uint16 {
	return node.kvPos(node.nkeys())
}

// nodeLookupLE gives the first child node whose range intersects the key where child[i] <= key.
// TODO: binary search
func nodeLookupLE(node BNode, key []byte) uint16 {
	nKeys := node.nkeys()
	found := uint16(0)

	// the first key is a copy from the parent node and is always
	// less than or equal to the key given nature of tree
	// TODO: notice the iterative nature here. move to binary search
	for i := uint16(1); i < nKeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// Updating Tree

// leafInsert first sets the header on the newNode and then appends KVs up to index, appends the new KV, and then appends the rest of the KVs.
// Utilized to insert new KVs.
func leafInsert(
	newNode, oldNode BNode,
	idx uint16,
	key, value []byte,
) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()+1)
	// the order on these methods DO matter
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                       // append KVs up to leaf insert
	nodeAppendKV(newNode, idx, 0, key, value)                          // append KV in wanted place
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nkeys()-idx) // append KVs after insert
}

// leafUpdate sets the header on the newNode and then appends KVs before and then after the updated KV.
// Utilized to update a KV.
func leafUpdate(
	newNode, oldNode BNode,
	idx uint16,
	key, value []byte,
) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys())
	// the order on these methods DO matter
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                             // append KVs up to leaf insert
	nodeAppendKV(newNode, idx, 0, key, value)                                // append KV in wanted place
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nkeys()-(idx+1)) // append KVs after insert
}

// nodeReplaceChildN replaces a link with up to n links.
func nodeReplaceChildN(
	tree *BTree,
	newNode, oldNode BNode,
	idx uint16,
	children ...BNode,
) {
	incoming := uint16(len(children))
	// header
	newNode.setHeader(BNODE_NODE, oldNode.nkeys()+incoming-1) // add incoming count accounting for zero-index
	nodeAppendRange(newNode, oldNode, 0, 0, idx)              // append nodes up to children index
	for i, childNode := range children {                      // append each of the n children
		nodeAppendKV(newNode, idx+uint16(i), tree.alloc(childNode), childNode.getKey(0), nil)
	}
	nodeAppendRange(newNode, oldNode, idx+incoming, idx+1, oldNode.nkeys()-(idx+1)) // append remaining KVs after added children nodes
}

// replace 2 adjacent links with 1
func nodeReplaceTwoChildren(new, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// nodeAppendKV copies a KV into the position given.
func nodeAppendKV(newNode BNode, idx uint16, pointer uint64, key, value []byte) {
	// set the pointers
	newNode.setPointer(idx, pointer)
	// KVs
	pos := newNode.kvPos(idx)
	binary.LittleEndian.PutUint16(newNode[pos:], uint16(len(key)))     // put key length
	binary.LittleEndian.PutUint16(newNode[pos+2:], uint16(len(value))) // put value length
	copy(newNode[pos+4:], key)                                         // put key
	copy(newNode[pos+4+uint16(len(key)):], value)                      // put value
	// add offset to offset list for next key
	newNode.setOffset(idx+1, newNode.getOffset(idx)+4+uint16(len(key)+len(value)))
}

// nodeAppendRange copies the KVs into a range of n from the oldNode into the new based on newDest.
func nodeAppendRange(newNode, oldNode BNode, newDest, oldSrc, n uint16) {
	assert(oldSrc+n <= oldNode.nkeys(), "node too small to copy KVs")
	assert(newDest+n <= newNode.nkeys(), "node too small to copy KVs")
	if n == 0 { // nothing to do here
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		newNode.setPointer(newDest+i, oldNode.getPointer(oldSrc+i)) // with i offset, iteratively set pointers
	}
	// offsets
	destBegin := newNode.getOffset(newDest)
	srcBegin := oldNode.getOffset(oldSrc)
	for i := uint16(1); i <= n; i++ { // range of [1,n)
		offset := destBegin + oldNode.getOffset(oldSrc+i) - srcBegin // offset with i and offset begin from oldNode
		newNode.setOffset(newDest+i, offset)
	}
	// KVs
	begin := oldNode.kvPos(oldSrc)
	end := oldNode.kvPos(oldSrc + n)                           // end is marked by the range marker of n
	copy(newNode[newNode.kvPos(newDest):], oldNode[begin:end]) // copy the range from begin to end from old node to new node at the KV position
}

// nodeBisect splits a bigger-than-allowed node in two. The second node always fits on a page.
func nodeBisect(left, right, old BNode) {
	assert(old.nkeys() >= 2, "old node has more than 2 keys - cannot bisect")

	// start with initial guess
	nLeft := old.nkeys() / 2 // naiive direct split in two

	// try to fit left half
	leftBytes := func() uint16 {
		return HEADER + 8*nLeft + 2*nLeft + old.getOffset(nLeft)
	}
	for leftBytes() > BTREE_PAGE_SIZE {
		nLeft--
	}
	assert(nLeft >= 1, "split resulted in empty node")

	// try to fit right half
	rightBytes := func() uint16 {
		return old.nBytes() - leftBytes() + HEADER // can calc off of left side
	}
	for rightBytes() > BTREE_PAGE_SIZE {
		nLeft++
	}
	assert(nLeft < old.nkeys(), "old node unchanged by split")
	nRight := old.nkeys() - nLeft // calc right side off of left

	left.setHeader(old.btype(), nLeft)
	right.setHeader(old.btype(), nRight)
	nodeAppendRange(left, old, 0, 0, nLeft)
	nodeAppendRange(right, old, 0, nLeft, nRight)
	// left half still could be too big
	assert(right.nBytes() <= BTREE_PAGE_SIZE, "resulting tight node of split is too large") // we know that right side should always be less than a page
}

// nodeSplit accepts a node with 3 outcomes:
//   - if the node is not too big, returns the original node
//   - if the node is too large, splits with 3 outcomes:
//   - splits into 2 resulting in 2 properly sized nodes
//   - splits one more time resulting in 3 properly sized nodes
//
// NOTE: these created nodes are in memory structures and thus temporary - nodeReplaceChildN actually allocates them
func nodeSplit(old BNode) (uint16, [3]BNode) { //TODO: the return here could be cleaned up i think
	if old.nBytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE] // ensure size
		return 1, [3]BNode{old}     // return the original node as it is not too large
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // note: double page size as it may be split again later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeBisect(left, right, old)
	if left.nBytes() <= BTREE_PAGE_SIZE { // right is properly sized, check left
		left = left[:BTREE_PAGE_SIZE]   // ensure size
		return 2, [3]BNode{left, right} // return left and right
	}
	leftLeft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeBisect(leftLeft, middle, left)
	assert(leftLeft.nBytes() <= BTREE_PAGE_SIZE, "resulting split is still too big")
	return 3, [3]BNode{leftLeft, middle, right}
}

// treeInsert inserts a KV into a node. The result may be split.
// The caller is responsible for deallocating the input node as well as splitting and allocating the resulting nodes.
func treeInsert(tree *BTree, node BNode, key, value []byte) BNode {
	newNode := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // resulting node can be too big and will be split if so

	// find where to insert new key
	idx := nodeLookupLE(node, key)
	// check node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf node, check if key exists
		if bytes.Equal(key, node.getKey(idx)) { // found key - update it
			leafUpdate(newNode, node, idx, key, value)
		} else { // key not found - insert just after position
			leafInsert(newNode, node, idx+1, key, value)
		}
	case BNODE_NODE:
		// internal node, insert node to a child node
		nodeInsert(tree, newNode, node, idx, key, value)
	default:
		panic("unknown node type")
	}

	return newNode
}

// nodeInsert deals with inserting a KV in an internal node.
func nodeInsert(
	tree *BTree, newNode, node BNode,
	idx uint16,
	key, value []byte,
) {
	kPtr := node.getPointer(idx)
	// recursively insert to the child node
	knode := treeInsert(tree, tree.get(kPtr), key, value)
	// split results
	nSplits, split := nodeSplit(knode)
	// deallocate child node
	tree.del(kPtr)
	// update links
	nodeReplaceChildN(tree, newNode, node, idx, split[:nSplits]...)
}

// remove a key from a leaf node
func leafDeleteKey(new, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// merge 2 nodes into 1
func nodeMerge(new, left, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())     // create with enough space for combined keyspace of right and left
	nodeAppendRange(new, left, 0, 0, left.nkeys())              // append up to left.nkeys()
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys()) // append after left.nkeys()
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// look up key
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf node
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDeleteKey(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("unknown node type: bad node")
	}
}

// delete a key from a BNODE_NODE type node
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the child
	kptr := node.getPointer(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr) // deallocate the pointer

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for needed merging
	mergeDirection, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDirection < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPointer(idx - 1))
		nodeReplaceTwoChildren(new, node, idx-1, tree.alloc(merged), merged.getKey(0))
	case mergeDirection > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPointer(idx + 1))
		nodeReplaceTwoChildren(new, node, idx, tree.alloc(merged), merged.getKey(0))
	case mergeDirection == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0, "empty child but no sibling")
		new.setHeader(BNODE_NODE, 0) // make parent empty as well
	case mergeDirection == 0 && updated.nkeys() > 0:
		nodeReplaceChildN(tree, new, node, idx, updated)
	}

	return new
}

// decides if the updated child should be merged with a sibling. Three outcomes:
//   - 1, right sibling
//   - 0, empty BNode
//   - -1, left sibling
//
// Deleted keys are unused space within the nodes. In bad cases, mostly empty trees can keep a large number of nodes.
// To clean this up, we set a threshold of BTREE_PAGE_SIZE/4 to trigger merges earlier.
//
// TODO: enumerate these returns to make this cleaner
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTREE_PAGE_SIZE/4 { // updated is too large and cannot be merged
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPointer(idx - 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE { // ensure that we are below the page limit
			return -1, sibling // left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPointer(idx + 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE { // ensure that we are below the page limit
			return 1, sibling // right
		}
	}

	return 0, BNode{}
}
