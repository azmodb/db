// Package db implements an immutable, consistent, in-memory key/value
// database. DB uses an immutable Left-Leaning Red-Black tree (LLRB)
// internally and supports snapshotting.
//
// Basic example:
//
//	db := New()
//	batch := db.Next()
//	batch.Insert([]byte("k1"), []byte("v1.1"), false)
//	batch.Insert([]byte("k1"), []byte("v1.2"), false)
//	batch.Insert([]byte("k1"), []byte("v1.3"), false)
//
//	batch.Put([]byte("k2"), []byte("v2.1"), false)
//	batch.Put([]byte("k2"), []byte("v2.2"), false)
//	batch.Commit()
//
//	fn := func(key []byte, rec *Record) bool {
//		fmt.Printf("%s - %s\n", key, rec.Values)
//		return false
//	}
//	db.Range(nil, nil, 0, true, fn)
//
package db

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/llrb"
)

var (
	// errRevisionNotFound is returned when trying to access a revision
	// that has not been created.
	errRevisionNotFound = errors.New("revision not found")

	// errKeyNotFound is returned when trying to access a key that has
	// not been created.
	errKeyNotFound = errors.New("key not found")

	// errIncompatibleValue is returned when trying create or delete a
	// value on an imcompatible key.
	errIncompatibleValue = errors.New("incompatible value")
)

// DB represents an immutable, consistent, in-memory key/value database.
// All access is performed through a batch with can be obtained through
// the database.
type DB struct {
	archive sync.Mutex // exclusive archive transaction
	writer  sync.Mutex // exclusive writer transaction
	tree    unsafe.Pointer

	mu  sync.RWMutex // protects notifier registry
	reg map[string]*notifier
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

// New returns an immutable, consistent, in-memory key/value database.
func New() *DB { return newDB(nil) }

func newDB(t *tree) *DB {
	if t == nil {
		t = &tree{root: &llrb.Tree{}}
	}
	return &DB{
		reg:  make(map[string]*notifier),
		tree: unsafe.Pointer(t),
	}
}

func (db *DB) store(t *tree) {
	atomic.StorePointer(&db.tree, unsafe.Pointer(t))
}

func (db *DB) load() *tree {
	return (*tree)(atomic.LoadPointer(&db.tree))
}

// Rev returns the current revision of the database.
func (db *DB) Rev() int64 {
	tree := db.load()
	return tree.rev
}

// Len returns the number of keys inside the database.
func (db *DB) Len() int {
	tree := db.load()
	return tree.root.Len()
}

func bcopy(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func grow(dst, src []byte) []byte {
	n := len(src)
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	copy(dst, src)
	return dst
}
