package db

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

var valuePool = sync.Pool{New: func() interface{} { return &Value{&pb.Value{}} }}

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

func (v *Value) Close() {
	if v == nil || v.Value == nil {
		return
	}
	if cap(v.Value.Data) > 8192 { // TODO
		v.Value.Data = v.Value.Data[:8192]
	}
	v.Value.Num = 0
	v.Value.Revs = nil
	valuePool.Put(v)
}

func (v *Value) IsNum() bool   { return v.Value.Data == nil }
func (v *Value) Bytes() []byte { return v.Value.Data }
func (v *Value) Num() int64    { return v.Value.Num }
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

// len returns the number of keys in the database. only usefull for unit
// testing.
func (db *DB) len() int {
	tree := db.load()
	return tree.root.Len()
}
