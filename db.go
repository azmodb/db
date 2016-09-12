// Package db implements an immutable, consistent, im-memory key/value
// database.
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

// DB represents an immutable, consistent, im-memory key/value database.
type DB struct {
	archive sync.Mutex // exclusive archive transaction
	writer  sync.Mutex // exclusive writer transaction
	tree    unsafe.Pointer
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

// New returns an immutable, consistent, im-memory key/value database.
func New() *DB { return newDB(nil) }

func newDB(t *tree) *DB {
	if t == nil {
		t = &tree{root: &llrb.Tree{}}
	}
	return &DB{tree: unsafe.Pointer(t)}
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

// Len returns the number of keys in the database.
func (db *DB) Len() int {
	tree := db.load()
	return tree.root.Len()
}
