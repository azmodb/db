package db

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

const (
	// ErrRevisionNotFound means that a get call did not find the requested
	// revision.
	ErrRevisionNotFound = Error("revision not found")

	// ErrKeyNotFound means that a get or watch call did not find the
	// requested key.
	ErrKeyNotFound = Error("key not found")
)

// Error represents a get or watch error.
type Error string

func (e Error) Error() string { return string(e) }

// Txn represents a write-only transaction on the database.
type Txn struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

// Put sets the value for the key in the database. If the key exists a
// new version will be created. Supplied key/value pair must remain
// valid for the life of the transaction.
//
// Put returns the previous value and revisions of the key/value pair
// if any and the current revision of the database.
func (t *Txn) Put(key, value []byte, ts bool) (*pb.Value, int64) {
	match := getPair(key)
	defer putPair(match)

	// TODO: optimize value bcopy
	var p *pair
	t.rev++
	if elem := t.txn.Get(match); elem != nil {
		p = elem.(*pair).copy() // TODO: optimize, if tombstone
		if ts {
			p.items = []item{item{data: bcopy(value), rev: t.rev}}
		} else {
			p.append(bcopy(value), t.rev)
		}
		t.txn.Insert(p)
	} else {
		p = newPair(bcopy(key), bcopy(value), t.rev)
		t.txn.Insert(p)
	}

	t.db.mu.RLock()
	if n, found := t.db.reg[string(key)]; found {
		n.Notify(bcopy(value), p.revs())
	}
	t.db.mu.RUnlock()

	return &pb.Value{
		Value:     value, // TODO: copy value here?
		Revisions: p.revs(),
	}, t.rev
}

// Delete remove a key from the database. If the key does not exist then
// nothing is done.
//
// Delete returns the previous value and revisions of the key/value pair
// if any and the current revision of the database.
func (t *Txn) Delete(key []byte) (*pb.Value, int64) {
	match := getPair(key)
	defer putPair(match)

	if elem := t.txn.Get(match); elem != nil {
		p := elem.(*pair)
		t.rev++
		t.txn.Delete(match)

		t.db.mu.RLock()
		if n, found := t.db.reg[string(key)]; found {
			n.Close()
		}
		t.db.mu.RUnlock()

		return &pb.Value{
			Value:     bcopy(p.last().data),
			Revisions: p.revs(),
		}, t.rev
	}
	return nil, t.rev
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

// DB is a consistent in-memory key/value store.
type DB struct {
	writer sync.Mutex // exclusive writer transaction
	tree   unsafe.Pointer

	mu  sync.RWMutex // protects notifier registry
	reg map[string]*notifier
}

// New returns a DB.
func New() *DB {
	return &DB{
		tree: unsafe.Pointer(&tree{
			root: &llrb.Tree{},
			rev:  0,
		}),
		reg: make(map[string]*notifier),
	}
}

// Txn starts a new transaction. One transaction can be used at a time.
// Starting multiple transactions will cause the calls to block and be
// serialized until the current transaction finishes. Transactions should
// not be dependent on one another.
//
// You must commit or rollback transactions after you are finished!
func (db *DB) Txn() *Txn {
	db.writer.Lock()
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return &Txn{
		txn: tree.root.Txn(),
		rev: tree.rev,
		db:  db,
	}
}

// Commit closes the transaction and writes all changes into the
// database.
func (t *Txn) Commit() {
	if t.db == nil || t.txn == nil { // already aborted or committed
		return
	}

	tree := &tree{
		root: t.txn.Commit(),
		rev:  t.rev,
	}

	atomic.StorePointer(&t.db.tree, unsafe.Pointer(tree))
	t.txn = nil
	t.rev = 0
	t.db.writer.Unlock() // release the writer lock
	t.db = nil
}

// Rollback closes the transaction and ignores all previous updates.
func (t *Txn) Rollback() {
	if t.db == nil || t.txn == nil { // already aborted or committed
		return
	}

	t.txn = nil
	t.db.writer.Unlock() // release the writer lock
	t.db = nil
}

// Get gets the value for the given key and revision. If revision <= 0
// Get returns the last version.
//
// Get returns the current revision of the database. If there is an
// error it will be of type ErrKeyNotFound of ErrRevisionNotFound.
func (db *DB) Get(key []byte, rev int64) (*pb.Value, int64, error) {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	match := getPair(key)
	defer putPair(match)

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		if rev > 0 {
			item, found := p.findByRev(rev)
			if !found {
				return nil, tree.rev, ErrRevisionNotFound
			}
			return &pb.Value{
				Value:     bcopy(item.data),
				Revisions: p.revs(),
			}, tree.rev, nil
		}

		return &pb.Value{
			Value:     bcopy(p.last().data),
			Revisions: p.revs(),
		}, tree.rev, nil
	}
	return nil, tree.rev, ErrKeyNotFound
}

// Func is a function that operates on a key range. If done is returned
// true, the Func is indicating that no further work needs to be done
// and so the traversal function should traverse no further.
type Func func(pair *pb.Pair, rev int64) (done bool)

func (db *DB) Range(from, to []byte, rev int64, fn Func) int64 {
	tree := (*tree)(atomic.LoadPointer(&db.tree))

	f := func(elem llrb.Element) bool {
		p := elem.(*pair)
		var item item
		if rev > 0 {
			var found bool
			item, found = p.findByRev(rev)
			if !found {
				return false
			}
		} else {
			item = p.last()
		}

		return fn(&pb.Pair{
			Key:       bcopy(p.key),
			Value:     bcopy(item.data),
			Revisions: p.revs(),
		}, tree.rev)
	}

	fromPair, toPair := getPair(from), getPair(to)
	defer func() {
		putPair(fromPair)
		putPair(toPair)
	}()
	tree.root.Range(fromPair, toPair, f)
	return tree.rev
}

// Watch returns a new watcher for the give key and the current
// revision of the database.
//
// If there is an error it will be of type ErrKeyNotFound.
func (db *DB) Watch(key []byte) (*Watcher, int64, error) {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	match := getPair(key)
	defer putPair(match)

	if elem := tree.root.Get(match); elem != nil {
		db.mu.Lock()
		n, found := db.reg[string(key)]
		if !found {
			n = newNotifier()
			db.reg[string(key)] = n
		}

		watcher := n.Add()
		db.mu.Unlock()

		return watcher, tree.rev, nil
	}
	return nil, tree.rev, ErrKeyNotFound
}

// Rev returns the current revision of the database.
func (db *DB) Rev() int64 {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return tree.rev
}

// Len returns the number of elemets in the database.
func (db *DB) Len() int {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return tree.root.Len()
}

func bcopy(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
