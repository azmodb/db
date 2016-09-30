package db

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/db/pb"
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

var (
	recPool = sync.Pool{New: func() interface{} { return &Record{} }}
	evPool  = sync.Pool{New: func() interface{} { return &Event{} }}
)

// DB represents an immutable, consistent, in-memory key/value database.
// All write access is performed through a batch with can be obtained
// through the database.
type DB struct {
	writer   sync.Mutex // exclusive writer transaction
	tree     unsafe.Pointer
	writable bool

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

type Record struct {
	*pb.Record
}

func newRecord(rec *pb.Record) *Record {
	r := recPool.Get().(*Record)
	r.Record = rec
	return r
}

func (r *Record) Close() {
	if r == nil || r.Record == nil {
		return
	}

	rec := r.Record
	closeProtobufRecord(rec)
	r.Record = nil
	r = nil
}

func (r *Record) IsNum() bool { return r.Type == pb.Record_Numeric }

type Event struct {
	current int64
	*pb.Record
}

func newEvent(current int64, rec *pb.Record) *Event {
	e := evPool.Get().(*Event)
	e.current = current
	e.Record = rec
	return e
}

func (e *Event) Close() {
	if e == nil || e.Record == nil {
		return
	}

	rec := e.Record
	closeProtobufRecord(rec)
	e.Record = nil
	e.current = 0
	e = nil
}

func (e *Event) IsNum() bool { return e.Type == pb.Record_Numeric }

// Rev returns the revision this event happend.
func (e *Event) Rev() int64 { return e.current }

type RangeFunc func(key string, ev *Event) (done bool)

func walk(end string, rev, current int64, history bool, fn RangeFunc) llrb.Visitor {
	return func(elem llrb.Element) bool {
		p := elem.(*pair)
		if end != "" && p.key >= end {
			return true
		}

		var rec *pb.Record
		if rev > 0 {
			index, found := p.find(rev, false)
			if !found { // revision not found
				return false
			}
			if history {
				rec = p.from(index)
			} else {
				rec = p.last()
			}
		} else {
			if history {
				rec = p.from(0)
			} else {
				rec = p.last()
			}
		}
		return fn(p.key, newEvent(current, rec))
	}
}

func (db *DB) Range(from, to string, rev int64, history bool, fn RangeFunc) {
	if from > to {
		return // invalid key sarch query range, report nothing
	}

	tree := db.load()
	if from == "" && to == "" {
		tree.root.ForEach(walk("", rev, tree.rev, history, fn))
		return
	}
	if from == "" && to != "" {
		tree.root.ForEach(walk(to, rev, tree.rev, history, fn))
		return
	}

	fmatch, tmatch := newMatcher(from), newMatcher(to)
	defer func() {
		fmatch.release()
		tmatch.release()
	}()

	tree.root.Range(fmatch, tmatch, walk("", rev, tree.rev, history, fn))
}

func (db *DB) Get(key string, rev int64, history bool) (*Record, int64, error) {
	match := newMatcher(key)
	defer match.release()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		var rec *pb.Record
		p := elem.(*pair)

		if rev > 0 {
			index, found := p.find(rev, true)
			if !found {
				return nil, tree.rev, errRevisionNotFound
			}
			if history {
				rec = p.from(index)
			} else {
				rec = p.at(index)
			}
		} else {
			if history {
				rec = p.from(0)
			} else {
				rec = p.last()
			}
		}
		return newRecord(rec), tree.rev, nil
	}
	return nil, tree.rev, errKeyNotFound
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
