// Package db implements an immutable, consistent, in-memory key/value database.
package db

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

var valuePool = sync.Pool{New: func() interface{} { return &Value{&pb.Value{}} }}

const maxPooledLen = 8192 // TODO: find better value

// Value represents a read-only key/value database query result. The
// caller must close the value when done with it.
type Value struct {
	*pb.Value
}

func newValue(data interface{}, revs []int64) *Value {
	v := valuePool.Get().(*Value)
	switch t := data.(type) {
	case []byte:
		if cap(v.Value.Data) < len(t) {
			v.Value.Data = make([]byte, len(t))
		}
		v.Value.Data = v.Value.Data[:len(t)]
		copy(v.Value.Data, t)
	case int64:
		v.Value.Num = t
	default:
		panic("invalid value type")
	}

	v.Value.Revs = revs
	return v
}

// Close closes the value, rendering it unusable. The caller must close
// the value when done with it.
func (v *Value) Close() {
	if cap(v.Value.Data) > maxPooledLen {
		v.Value.Data = v.Value.Data[0:0:maxPooledLen]
	}
	v.Value.Num = 0
	v.Value.Revs = nil
	valuePool.Put(v)
}

// IsNum returns true if this value is a numeric value.
func (v *Value) IsNum() bool { return v.Value.Data == nil }

// Bytes returns the underlying value byte slice. If Value is a numeric
// value Bytes returns nil.
func (v *Value) Bytes() []byte { return v.Value.Data }

// Num returns the underlying integer. If Value is a byte slice value
// Num returns 0.
func (v *Value) Num() int64 { return v.Value.Num }

// Revs returns the revision numbers of this value.
func (v *Value) Revs() []int64 { return v.Value.Revs }

type DB struct {
	writer sync.Mutex // exclusive writer transaction
	tree   unsafe.Pointer
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

// New returns a consistent in-memory key/value database.
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

func (db *DB) Next() *Batch {
	db.writer.Lock()
	tree := db.load()
	return &Batch{txn: tree.root.Txn(), rev: tree.rev, db: db}
}

// Rev returns the current revision of the database.
func (db *DB) Rev() int64 {
	tree := db.load()
	return tree.rev
}

// len returns the number of keys in the database. only usefull for
// unit esting.
func (db *DB) len() int {
	tree := db.load()
	return tree.root.Len()
}
