package db

import (
	"errors"

	"github.com/azmodb/llrb"
)

type Batch struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

var errInvalidDataType = errors.New("invalid data type")

func (b *Batch) insert(key []byte, value interface{}, ts, prev bool) (*Record, error) {
	match := newMatcher(key)
	defer match.Close()

	b.rev++ // increment batch revision
	var p, parent *pair
	if elem := b.txn.Get(match); elem != nil {
		_, isNum := value.(int64)
		parent = elem.(*pair)
		p = parent.clone()
		if (isNum && !p.isNum()) || (!isNum && p.isNum()) {
			return nil, errInvalidDataType
		}

		if !p.isNum() {
			if ts {
				p.tombstone(value, b.rev)
			} else {
				p.append(value, b.rev)
			}
		} else {
			p.increment(value, b.rev)
		}
	} else { // element does not exists
		p = newPair(key, value, b.rev)
	}
	b.txn.Insert(p)

	if !prev || parent == nil {
		return nil, nil
	}
	return parent.last(b.rev), nil
}

func (b *Batch) Decrement(key []byte, value int64, prev bool) (*Record, error) {
	rec, err := b.insert(key, value*-1, false, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

func (b *Batch) Increment(key []byte, value int64, prev bool) (*Record, error) {
	rec, err := b.insert(key, value, false, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

func (b *Batch) Insert(key []byte, value []byte, prev bool) (*Record, error) {
	rec, err := b.insert(key, value, false, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

func (b *Batch) Put(key []byte, value []byte, prev bool) (*Record, error) {
	rec, err := b.insert(key, value, true, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

func (b *Batch) Delete(key []byte) (*Record, error) {
	return nil, nil
}

func (b *Batch) Rev() int64 { return b.rev }

func (db *DB) Next() *Batch {
	db.writer.Lock()
	tree := db.load()
	return &Batch{txn: tree.root.Txn(), rev: tree.rev, db: db}
}

// Commit closes the transaction and writes all changes into the
// database.
func (b *Batch) Commit() {
	if b.txn == nil { // already aborted or committed
		return
	}

	tree := &tree{root: b.txn.Commit(), rev: b.rev}
	b.db.store(tree)
	b.txn = nil
	b.rev = 0
	b.db.writer.Unlock() // release the writer lock
	b.db = nil
}

// Rollback closes the transaction and ignores all previous updates.
func (b *Batch) Rollback() {
	if b.txn == nil { // already aborted or committed
		return
	}

	b.txn = nil
	b.db.writer.Unlock() // release the writer lock
	b.db = nil
}
