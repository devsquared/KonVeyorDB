# B+Tree Structure
1. A fixed-size header, which contains:
    -  The type of node (leaf or internal).
    -  The number of keys.
2. A list of pointers to child nodes for internal nodes.
3. A list of KV pairs.
4. A list of offsets to KVs, which can be used to binary search KVs.
Looks like: 
| type | nkeys |  pointers  |   offsets  | key-values | unused |
|  2B  |  2B   | nkeys * 8B | nkeys * 2B |    ...     |        |

With KV pairs looking like:
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |

## TODO: 
1. Design a node format that contains all the necessary bits.
2. Manipulate nodes in a copy-on-write fashion (insert and delete keys).
3. Split and merge nodes.
4. Tree insertion and deletion.


## Notes on simplification
As we are exploring the fundamentals of a DB, there are simplifications. Listed are some notes from the book we are working from.

> The same format is used for both leaf nodes and internal nodes. This wastes some space:
> leaf nodes don’t need pointers and internal nodes don’t need values.

> An internal node of 𝑛 branches contains 𝑛 keys, each key is duplicated from the minimum
> key of the corresponding subtree. However, only 𝑛 − 1 keys are needed for 𝑛 branches, as
> you’ll see in other B-tree introductions. The extra key makes the visualization easier.

> We’ll set the node size to 4K, which is the typical OS page size. However, keys and values
> can be arbitrarily large, exceeding a single node. There should be a way to store large
> KVs outside of nodes, or to make the node size variable. This problem is solvable, but not
> fundamental. So we’ll skip it by limiting the KV size so that they always fit inside a node.