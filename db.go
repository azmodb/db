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
func New() *DB {
	return &DB{
		tree: unsafe.Pointer(&tree{root: &llrb.Tree{}}),
	}
}

type RangeFunc func(key []byte, value *Value, rev int64) (done bool)

func rangeFunc(wantRev int64, fn RangeFunc) llrb.Visitor {
	return func(elem llrb.Element) bool {
		var data interface{}
		var rev int64
		p := elem.(*pair)

		if wantRev > 0 {
			// TODO: find does not work here, need p.smallerThan(wantRev)
			/*
				var found bool
				data, rev, found = p.find(wantRev)
				if !found {
					return false
				}
			*/
		} else {
			data, rev = p.last()
		}
		switch t := data.(type) { // we need to copy the key here
		case []byte:
			return fn(bcopy(p.key), newValue(t, p.revs()), rev)
		case int64:
			return fn(bcopy(p.key), newValue(t, p.revs()), rev)
		}
		panic("invalid value type")
	}
}

func (db *DB) Range(from, to []byte, rev int64, fn RangeFunc) int64 {
	fromMatch, toMatch := newMatcher(from), newMatcher(to)
	defer func() {
		fromMatch.Close()
		toMatch.Close()
	}()

	tree := (*tree)(atomic.LoadPointer(&db.tree))
	tree.root.Range(fromMatch, toMatch, rangeFunc(rev, fn))
	return tree.rev
}

func (db *DB) Get(key []byte, rev int64) (*Value, int64) {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	match := newMatcher(key)
	defer match.Close()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		if rev > 0 {
			data, _, found := p.find(rev)
			if !found {
				return nil, tree.rev // revision not found
			}
			return newValue(data, p.revs()), tree.rev
		}
		data, _ := p.last()
		return newValue(data, p.revs()), tree.rev
	}
	return nil, tree.rev // key not found
}

func (db *DB) Next() *Batch {
	db.writer.Lock()
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return &Batch{txn: tree.root.Txn(), rev: tree.rev, db: db}
}

// Rev returns the current revision of the database.
func (db *DB) Rev() int64 {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return tree.rev
}

// len returns the number of keys in the database. only usefull for unit
// testing.
func (db *DB) len() int {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return tree.root.Len()
}
