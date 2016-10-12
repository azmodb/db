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

	// notifierCanceled is the error returned when the watcher is
	// canceled.
	notifierCanceled = perror("watcher shut down")

	// errInvertedRange is returned when a inverted range is supplied.
	errInvertedRange = perror("inverted range")
)

type perror string

func (e perror) Error() string { return string(e) }

// DB represents an immutable, consistent, in-memory key/value database.
// All access is performed through a transaction which can be obtained
// through the database.
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

// Get retrieves the value for a key at revision rev. If rev <= 0 it
// returns the current value for a key. If equal is true the value
// revision must match the supplied rev.
func (db *DB) Get(key []byte, rev int64, equal bool) (interface{}, int64, int64, error) {
	match := newMatcher(key)
	defer match.release()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		b, found := lookup(p, rev, equal)
		if found {
			return b.data, b.rev, tree.rev, nil
		}
		return nil, 0, tree.rev, errRevisionNotFound
	}
	return nil, 0, tree.rev, errKeyNotFound
}

func lookup(p *pair, rev int64, equal bool) (block, bool) {
	var b block
	if rev > 0 {
		index, found := p.find(rev, equal)
		if !found {
			return b, false
		}
		b = p.at(index)
	} else {
		b = p.last()
	}
	return b, true
}

func rangeFunc(n *Notifier, rev int64, current int64, limit int32) llrb.Visitor {
	return func(elem llrb.Element) bool {
		p := elem.(*pair)
		b, found := lookup(p, rev, false)
		if found {
			return !n.send(p.key, b.data, b.rev, current)
		}
		return false // ignore revision not found error
	}
}

func (db *DB) get(tree *tree, key []byte, rev int64) (*Notifier, int64, error) {
	n := newNotifier(42, nil, 1)
	go func() {
		data, created, current, err := db.Get(key, rev, false)
		if err != nil {
			n.close(err)
		} else {
			n.send(key, data, created, current)
		}
	}()
	return n, tree.rev, nil
}

// Range iterates over values stored in the database in the range at rev
// over the interval [from, to] from left to right. Limit limits the
// number of keys returned. If rev <= 0 Range gets the keys at the
// current revision of the database. From/To combination:
//
//	from == nil && to == nil:
//		the request returns all keys in the database
//	from != nil && to != nil:
//		the request returns the keys in the interval
//	from != nil && to == nil:
//		the request returns the key (like Get)
//
// Range a notifier, the current revision of the database and an error
// if any.
func (db *DB) Range(from, to []byte, rev int64, limit int32) (*Notifier, int64, error) {
	tree := db.load()
	if compare(from, to) > 0 {
		return nil, tree.rev, errInvertedRange
	}

	if from != nil && to == nil { // simulate get request with equal == false
		return db.get(tree, from, rev)
	}

	n := newNotifier(42, nil, defaultNotifierCapacity)
	go func() {
		defer n.Cancel() // in any case cancel the infinte event queue

		if from == nil && to == nil { // foreach request
			tree.root.ForEach(rangeFunc(n, rev, tree.rev, limit))
			return
		}

		lo, hi := newMatcher(from), newMatcher(to)
		defer func() {
			lo.release()
			hi.release()
		}()
		tree.root.Range(lo, hi, rangeFunc(n, rev, tree.rev, limit))
	}()

	return n, tree.rev, nil
}

// Rev returns the current revision of the database.
func (db *DB) Rev() int64 {
	tree := db.load()
	return tree.rev
}

// Watch returns a notifier for a key. If the key does not exist it
// returns an error.
func (db *DB) Watch(key []byte) (*Notifier, int64, error) {
	match := newMatcher(key)
	defer match.release()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		return p.stream.Register(), tree.rev, nil
	}
	return nil, tree.rev, errKeyNotFound
}

// Txn starts a new batch transaction. Only one batch transaction can
// be used at a time. Starting multiple batch transactions will cause
// the calls to block and be serialized until the current transaction
// finishes.
func (db *DB) Txn() *Txn {
	db.writer.Lock()
	tree := db.load()
	return &Txn{txn: tree.root.Txn(), rev: tree.rev, db: db}
}

// Txn represents a batch transaction on the database.
type Txn struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

// Updater is a function that operates on a key/value pair
type Updater func(data interface{}) interface{}

// Update updates the value for a key. If the key exists and tombstone is
// true then its previous versions will be overwritten. Supplied key
// and value must remain valid for the life of the database.
//
// It the key exists and the value data type differ it returns an error.
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

func noop(data interface{}) Updater {
	return func(_ interface{}) interface{} {
		return data
	}
}

// Put sets the value for a key. If the key exists and tombstone is true
// then its previous versions will be overwritten. Supplied key and
// value must remain valid for the life of the database.
//
// It the key exists and the value data type differ, it returns an error.
func (tx *Txn) Put(key []byte, data interface{}, tombstone bool) (int64, error) {
	return tx.Update(key, noop(data), tombstone)
}

// Delete removes a key/value pair and returns the current revision of the
// database.
func (tx *Txn) Delete(key []byte) int64 {
	match := newMatcher(key)
	defer match.release()

	if elem := tx.txn.Get(match); elem != nil {
		p := elem.(*pair)
		tx.txn.Delete(p)
		tx.rev++
		p.stream.Notify(p, tx.rev)
		p.stream.Cancel()
	}
	return tx.rev
}

// Commit closes the transaction and writes all changes into the
// database.
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

// Rollback closes the transaction and ignores all previous updates.
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
