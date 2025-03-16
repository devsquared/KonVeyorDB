# KonVeyorDB
Simple Relational KV DB built in go. 

NOTE: this code only works on unix based machines. will not compile on windows machines

- at chapter 6.6

## Book
Working from the "Build-your-own" series. See [here](https://build-your-own.org/).

## TODO
- [ ] persistent KV based on B+Trees
    - [ ] Code B+Tree structure
    - [ ] Move this structure to disk
    - [ ] Add free list for space recollection

# Ideas and structures
## B+Trees
See: https://en.wikipedia.org/wiki/B%2B_tree

Basic principles:
- n-ary tree, node size is limited by a constant.
- Same height for all leaves.
- Split and merge for insertion and deletion.

3 invariants to uphold when updating
1. Same height for all leaf nodes
2. Node size is bounded by some constant.
3. Node is not empty. 

This means we grow by splitting nodes with changes propagating all the way to the parent node. Similarly, 
we shrink the tree by merging nodes to a new sibling node and this change can also propagate to the parent.

## Queries
3 types
- Scan: find in whole data set
- Point queries: query index by specific key
- Range queries: query the index by a range