// Package db implements an immutable, consistent, in-memory key/value store.
// DB uses an immutable Left-Leaning Red-Black tree (LLRB) internally.
// The database provides Atomicity, Consistency and Isolation from ACID.
// Being that it is in-memory, it does not provide durability.
//
// The database provides the following:
//
//	* Multi-Version Concurrency Control (MVCC) - By leveraging immutable LLRB
//	  trees the database is able to support any number of concurrent readers
//	  without locking,  and allows a writer to make progress.
//
//	* Transaction Support - The database allows for rich transactions, in
//	  which multiple objects are inserted, updated or deleted. The database
//	  provides atomicity and isolation in ACID terminology, such that until
//	  commit the updates are not visible.
package db

import (
	"encoding/binary"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/azmodb/db/backend"
	"github.com/azmodb/llrb"
)

const (
	// ErrRevisionNotFound is returned when trying to access a revision
	// that has not been created.
	ErrRevisionNotFound = perror("revision not found")

	// ErrKeyNotFound is returned when trying to access a key that has
	// not been created.
	ErrKeyNotFound = perror("key not found")

	// ErrIncompatibleValue is returned when trying create or delete a
	// value on an imcompatible key.
	ErrIncompatibleValue = perror("incompatible value")

	// PairDeleted is the error returned by a watcher when the
	// underlying is deleted.
	PairDeleted = perror("key/value pair deleted")

	// NotifierCanceled is the error returned when the watcher is
	// canceled.
	NotifierCanceled = perror("notifier is shut down")

	// ErrInvertedRange is returned when a inverted range is supplied.
	ErrInvertedRange = perror("inverted range")
)

type perror string

func (e perror) Error() string { return string(e) }

// DB represents an immutable, consistent, in-memory key/value database.
// All access is performed through a transaction which can be obtained
// through the database.
type DB struct {
	writer  sync.Mutex // exclusive writer transaction
	tree    unsafe.Pointer
	backend backend.Backend
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

func newDB(t *tree) *DB {
	if t == nil {
		t = &tree{root: &llrb.Tree{}}
	}
	return &DB{tree: unsafe.Pointer(t)}
}

// Load reloads the immutable, consistent, in-memory key/value database
// from the underlying backend.
func Load(path string, timeout time.Duration, opts ...backend.Option) (*DB, error) {
	backend, err := backend.Open(path, timeout, opts...)
	if err != nil {
		return nil, err
	}
	db, err := reload(backend)
	if err != nil {
		return nil, err
	}
	db.backend = backend
	return db, nil
}

// New returns an immutable, consistent, in-memory key/value database.
func New() *DB { return newDB(nil) }

// reload reloads the immutable, consistent, in-memory key/value database
// from the underlying backend.
func reload(backend backend.Backend) (*DB, error) {
	rev, err := backend.Last()
	if err != nil {
		return nil, err
	}

	tree := &tree{
		rev:  int64(binary.BigEndian.Uint64(rev[:])),
		root: &llrb.Tree{},
	}
	txn := tree.root.Txn()

	err = backend.Range(rev, func(key, value []byte) (err error) {
		buf := newBuffer(value)
		blocks := []block{}
		if err = decode(buf, &blocks); err != nil {
			return err
		}

		p := &pair{
			stream: &stream{},
			blocks: blocks,
		}
		p.key = make([]byte, len(key))
		copy(p.key, key)

		txn.Insert(p)
		return err
	})
	if err != nil {
		return nil, err
	}

	tree.root = txn.Commit()
	return newDB(tree), nil
}

// Snapshot writes the entire in-memory database to the underlying
// backend.
func (db *DB) Snapshot() (int64, error) {
	return db.snapshot(db.backend)
}

func (db *DB) snapshot(backend backend.Backend) (int64, error) {
	tree := db.load()

	rev := [8]byte{}
	binary.BigEndian.PutUint64(rev[:], uint64(tree.rev))

	batch, err := backend.Batch(rev)
	if err != nil {
		return tree.rev, err
	}

	buf := newBuffer(nil)
	tree.root.ForEach(func(elem llrb.Element) bool {
		p := elem.(*pair)

		buf.Reset()
		if err = encode(buf, p.blocks); err != nil {
			return true
		}

		if err = batch.Put(p.key, buf.Bytes()); err != nil {
			return true
		}
		return false
	})
	if err != nil {
		batch.Close()
		return tree.rev, err
	}
	return tree.rev, batch.Close()
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
//
// Get returns the revision of the key/value pair, the current revision
// of the database and an errors if any.
func (db *DB) Get(key []byte, rev int64, equal bool) (interface{}, int64, int64, error) {
	match := newMatcher(key)
	defer match.release()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		b, found := lookup(p, rev, equal)
		if found {
			return b.Data, b.Rev, tree.rev, nil
		}
		return nil, 0, tree.rev, ErrRevisionNotFound
	}
	return nil, 0, tree.rev, ErrKeyNotFound
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
			return !n.send(p.key, b.Data, b.Rev, current)
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
// Range returns a notifier, the current revision of the database and an
// error if any.
func (db *DB) Range(from, to []byte, rev int64, limit int32) (*Notifier, int64, error) {
	tree := db.load()
	if compare(from, to) > 0 {
		return nil, tree.rev, ErrInvertedRange
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
	return nil, tree.rev, ErrKeyNotFound
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
		last := p.last().Data
		data := up(last)
		if !typeEqual(last, data) {
			return tx.rev, ErrIncompatibleValue
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
