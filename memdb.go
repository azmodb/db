// Package db implements an immutable, consistent, in-memory key/value store.
// DB uses an immutable Left-Leaning Red-Black tree (LLRB) internally.
package db

import (
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/llrb"
)

const (
	// errRevisionNotFound is returned when trying to access a revision
	// that has not been created.
	errRevisionNotFound = perror("revision not found")

	// errKeyNotFound is returned when trying to access a key that has
	// not been created.
	errKeyNotFound = perror("key not found")

	// errIncompatibleValue is returned when trying create or delete a
	// value on an imcompatible key.
	errIncompatibleValue = perror("incompatible value")

	// pairDeleted is the error returned by a watcher when the
	// underlying is deleted.
	pairDeleted = perror("key/value pair deleted")

	// watcherCanceled is the error returned when the watcher is
	// canceled.
	watcherCanceled = perror("watcher shut down")
)

type perror string

func (e perror) Error() string { return string(e) }

type DB struct {
	writer sync.Mutex // exclusive writer transaction
	tree   unsafe.Pointer
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
	return &DB{tree: unsafe.Pointer(t)}
}

func (db *DB) store(t *tree) {
	atomic.StorePointer(&db.tree, unsafe.Pointer(t))
}

func (db *DB) load() *tree {
	return (*tree)(atomic.LoadPointer(&db.tree))
}

func (db *DB) Get(key []byte, rev int64, equal bool) (interface{}, int64, int64, error) {
	match := newMatcher(key)
	defer match.release()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		var b block
		if rev > 0 {
			index, found := p.find(rev, equal)
			if !found {
				return nil, 0, tree.rev, errRevisionNotFound
			}
			b = p.at(index)
		} else {
			b = p.last()
		}
		return b.data, b.rev, tree.rev, nil
	}
	return nil, 0, tree.rev, errKeyNotFound
}

func (db *DB) Range(from, to []byte, rev int64, limit int32) (<-chan Event, int64) {
	w := newWatcher(42, nil)
	tree := db.load()

	go func() {
		defer w.Close()

		rangeFunc := func(elem llrb.Element) bool {
			p := elem.(*pair)
			var b block
			if rev > 0 {
				index, found := p.find(rev, false)
				if !found {
					return false // ignore revision not found error
				}
				b = p.at(index)
			} else {
				b = p.last()
			}

			w.send(Event{
				Current: tree.rev,
				Created: b.rev,
				Data:    b.data,
				Key:     p.key,
			})
			return false
		}

		if from == nil && to == nil {
			tree.root.ForEach(rangeFunc)
			return
		}

		lo, hi := newMatcher(from), newMatcher(to)
		defer func() {
			lo.release()
			hi.release()
		}()

		tree.root.Range(lo, hi, rangeFunc)
	}()

	return w.Recv(), tree.rev
}

func (db *DB) Len() int {
	tree := db.load()
	return tree.root.Len()
}

func (db *DB) Rev() int64 {
	tree := db.load()
	return tree.rev
}

func (db *DB) Watch(key []byte) (*Watcher, int64, error) {
	match := newMatcher(key)
	defer match.release()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		return p.stream.Register(), tree.rev, nil
	}
	return nil, tree.rev, errKeyNotFound
}

func (db *DB) Txn() *Txn {
	db.writer.Lock()
	tree := db.load()
	return &Txn{txn: tree.root.Txn(), rev: tree.rev, db: db}
}

type Txn struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

type Updater func(data interface{}) interface{}

func (tx *Txn) Update(key []byte, up Updater, tombstone bool) (int64, error) {
	match := newMatcher(key)
	defer match.release()

	rev := tx.rev + 1
	var p *pair
	if elem := tx.txn.Get(match); elem != nil {
		p = elem.(*pair)
		last := p.last().data
		data := up(last)
		if !typeEqual(last, data) {
			return tx.rev, errIncompatibleValue
		}
		p = p.insert(data, rev, tombstone)
	} else {
		p = newPair(key, up(nil), rev)
	}
	tx.txn.Insert(p)
	tx.rev = rev
	p.stream.Notify(p, rev)

	return tx.rev, nil
}

func (tx *Txn) Put(key []byte, data interface{}, tombstone bool) (int64, error) {
	match := newMatcher(key)
	defer match.release()

	rev := tx.rev + 1
	var p *pair
	if elem := tx.txn.Get(match); elem != nil {
		p = elem.(*pair)
		last := p.last().data
		if !typeEqual(last, data) {
			return tx.rev, errIncompatibleValue
		}
		p = p.insert(data, rev, tombstone)
	} else {
		p = newPair(key, data, rev)
	}
	tx.txn.Insert(p)
	tx.rev = rev
	p.stream.Notify(p, rev)

	return tx.rev, nil
}

func (tx *Txn) Delete(key []byte) int64 {
	match := newMatcher(key)
	defer match.release()

	if elem := tx.txn.Get(match); elem != nil {
		p := elem.(*pair)
		tx.txn.Delete(p)
		tx.rev++
		p.stream.Notify(p, tx.rev)
		p.stream.Close()
	}
	return tx.rev
}

func (tx *Txn) Len() int { return tx.txn.Len() }

func (tx *Txn) Commit() {
	if tx.txn == nil { // already aborted or committed
		return
	}

	tree := &tree{root: tx.txn.Commit(), rev: tx.rev}
	tx.db.store(tree)
	tx.txn = nil
	tx.rev = 0
	tx.db.writer.Unlock() // release the writer lock
	tx.db = nil
}

func (tx *Txn) Rollback() {
	if tx.txn == nil { // already aborted or committed
		return
	}

	tx.txn = nil
	tx.db.writer.Unlock() // release the writer lock
	tx.db = nil
}

func typeEqual(a, b interface{}) bool {
	at, bt := reflect.TypeOf(a), reflect.TypeOf(b)
	ak, bk := at.Kind(), bt.Kind()
	if ak != bk {
		return false
	}
	if ak == reflect.Slice ||
		ak == reflect.Array ||
		ak == reflect.Chan ||
		ak == reflect.Map ||
		ak == reflect.Ptr {
		if at.Elem() != bt.Elem() {
			println("x")
			return false
		}
	}
	return true
}
