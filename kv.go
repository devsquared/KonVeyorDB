package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
)

type KVStore struct {
	Path  string
	Fsync func(int) error // overridable; for testing //TODO: does this need to be public if only needed for testing

	// internals
	fd   int
	tree BTree
	mmap struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
	failed bool // flag to mark if the last update failed or not
}

func (db *KVStore) Open() error {
	if db.Fsync == nil {
		db.Fsync = syscall.Fsync
	}
	var err error
	// B+tree callbacks
	db.tree.get = db.pageRead
	db.tree.alloc = db.pageAppend
	db.tree.del = func(uint64) {}
	// open or create the DB file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}
	// get the file size
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}
	// create the initial mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}
	// read the meta page
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	return nil
	// error
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// Magic number used for signature on files
const DB_SIG = "BuildYourOwnDB06"

// the 1st page stores the root pointer and other auxiliary data.
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func loadMeta(db *KVStore, data []byte) {
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

func saveMeta(db *KVStore) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func readRoot(db *KVStore, fileSize int64) error {
	if fileSize%BTREE_PAGE_SIZE != 0 {
		return errors.New("file is not a multiple of pages")
	}
	if fileSize == 0 { // empty file
		db.page.flushed = 1 // the meta page is initialized on the 1st write
		return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// verify the page
	bad := !bytes.Equal([]byte(DB_SIG), data[:16])
	// pointers are within range?
	maxpages := uint64(fileSize / BTREE_PAGE_SIZE)
	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxpages)
	bad = bad || !(0 < db.tree.root && db.tree.root < db.page.flushed)
	if bad {
		return errors.New("bad meta page")
	}
	return nil
}

// update the meta page. it must be atomic.
func updateRoot(db *KVStore) error {
	// NOTE: _likely_ power-loss atomic at the hardware level.
	// Similarly handled in Postgres: https://www.postgresql.org/message-id/flat/17064-bb0d7904ef72add3%40postgresql.org
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// open or create a file and fsync the directory
func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	// fsync the directory
	err = syscall.Fsync(dirfd)
	if err != nil { // may leave an empty file
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	// done
	return fd, nil
}

// `BTree.get`, read a page.
func (db *KVStore) pageRead(ptr uint64) []byte {
	assert(ptr < db.page.flushed+db.page.nappend, "invalid pointer")
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	return db.pageReadFile(ptr)
}

func (db *KVStore) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

// `BTree.new`, allocate a new page.
func (db *KVStore) pageAlloc(node []byte) uint64 {
	assert(len(node) == BTREE_PAGE_SIZE, "node is not page sized")
	if ptr := db.free.PopHead(); ptr != 0 { // try the free list
		assert(db.page.updates[ptr] == nil, "page updates pointer must be nil to create")
		db.page.updates[ptr] = node
		return ptr
	}
	return db.pageAppend(node) // append
}

// `FreeList.new`, append a new page.
func (db *KVStore) pageAppend(node []byte) uint64 {
	assert(len(node) == BTREE_PAGE_SIZE, "node is not page sized")
	ptr := db.page.flushed + db.page.nappend
	db.page.nappend++
	assert(db.page.updates[ptr] == nil, "page updates pointer must be nil to create")
	db.page.updates[ptr] = node
	return ptr
}

// `FreeList.set`, update an existing page.
func (db *KVStore) pageWrite(ptr uint64) []byte {
	assert(ptr < db.page.flushed+db.page.nappend, "invalid pointer")
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	node := make([]byte, BTREE_PAGE_SIZE)
	copy(node, db.pageReadFile(ptr)) // initialized from the file
	db.page.updates[ptr] = node
	return node
}

// extend the mmap by adding new mappings.
func extendMmap(db *KVStore, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<20) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func writePages(db *KVStore) error {
	// extend the mmap if needed
	size := (int(db.page.flushed) + len(db.page.temp)) * BTREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}
	// write data pages to the file
	offset := int64(db.page.flushed * BTREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}
	// discard in-memory data
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func (db *KVStore) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KVStore) Set(key, value []byte) error {
	db.tree.Insert(key, value)

	panic("not implemented")
	return updateFile(db)
}

func (db *KVStore) Delete(key []byte) (bool, error) {
	meta := saveMeta(db)
	if deleted, err := db.tree.Delete(key); !deleted {
		return false, err
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

func updateFile(db *KVStore) error {
	// 1. Write new nodes.
	if err := writePages(db); err != nil {
		return err
	}
	// 2. `fsync` to enforce the order between 1 and 3.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically.
	if err := updateRoot(db); err != nil {
		return err
	}
	// 4. `fsync` to make everything persistent.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	return nil
}
