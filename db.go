package db

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

const (
	ErrRevisionNotFound = Error("revision not found")
	ErrKeyNotFound      = Error("key not found")
)

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
// Put returns the previous revisions of the key/value pair if any and
// the current revision of the database.

func (t *Txn) Put(key, value []byte, ts bool) ([]int64, int64) {
	match := getPair(key)
	defer putPair(match)

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

	n, found := t.db.reg[string(key)]
	if found {
		n.notify(bcopy(value), p.revs())
	}
	return p.revs(), t.rev
}

// Delete remove a key from the database. If the key does not exist then
// nothing is done.
//
// Delete returns the previous revisions of the key/value pair if any
// and the current revision of the database.

func (t *Txn) Delete(key []byte) ([]int64, int64) {
	match := getPair(key)
	defer putPair(match)

	if elem := t.txn.Get(match); elem != nil {
		p := elem.(*pair)
		t.rev++
		t.txn.Delete(match)
		return p.revs(), t.rev
	}
	return nil, t.rev
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

type DB struct {
	writer sync.Mutex // exclusive writer transaction
	tree   unsafe.Pointer

	mu  sync.Mutex // protects notifier registry
	reg map[string]*notifier
}

func New() *DB {
	return &DB{
		tree: unsafe.Pointer(&tree{
			root: &llrb.Tree{},
			rev:  0,
		}),
		reg: make(map[string]*notifier),
	}
}

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

type Func func(pair *pb.Pair) bool

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
		})
	}

	fromPair, toPair := getPair(from), getPair(to)
	defer func() {
		putPair(fromPair)
		putPair(toPair)
	}()
	tree.root.Range(fromPair, toPair, f)
	return tree.rev
}

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

		watcher := n.create()
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
