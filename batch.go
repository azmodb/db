package db

import "github.com/azmodb/llrb"

// Batch represents a batch transaction on the database. A Batch is not
// thread safe.
type Batch struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

func (b *Batch) insert(key string, value interface{}, ts, prev bool) (*Record, int64, error) {
	match := newMatcher(key)
	defer match.release()

	rev := b.rev + 1 // increment batch revision
	var p, parent *pair
	if elem := b.txn.Get(match); elem != nil {
		parent = elem.(*pair)
		_, isNum := value.(int64)
		if (isNum && !parent.isNum()) || (!isNum && parent.isNum()) {
			return nil, b.rev, errIncompatibleValue
		}
		if !parent.isNum() {
			if ts {
				p = parent.tombstone(value, rev)
			} else {
				p = parent.insert(value, rev)
			}
		} else {
			p = parent.increment(value, rev)
		}
	} else {
		// Element does not exist. newPair create a copy if value is of type
		// []byte.
		p = newPair(key, value, rev)
	}

	b.txn.Insert(p)     // insert key/value pair
	b.db.notify(p, rev) // notify watchers
	b.rev = rev         // update batch revision

	if !prev || parent == nil {
		return nil, b.rev, nil
	}
	return newRecord(parent.last()), b.rev, nil
}

func (b *Batch) Decrement(key string, value int64, prev bool) (*Record, int64, error) {
	rec, rev, err := b.insert(key, value*-1, false, prev)
	if err != nil {
		rec.Close()
		return nil, rev, err
	}
	return rec, rev, nil
}

func (b *Batch) Increment(key string, value int64, prev bool) (*Record, int64, error) {
	rec, rev, err := b.insert(key, value, false, prev)
	if err != nil {
		rec.Close()
		return nil, rev, err
	}
	return rec, rev, nil
}

func (b *Batch) Insert(key string, value []byte, prev bool) (*Record, int64, error) {
	rec, rev, err := b.insert(key, value, false, prev)
	if err != nil {
		rec.Close()
		return nil, rev, err
	}
	return rec, rev, nil
}

func (b *Batch) Put(key string, value []byte, prev bool) (*Record, int64, error) {
	rec, rev, err := b.insert(key, value, true, prev)
	if err != nil {
		rec.Close()
		return nil, rev, err
	}
	return rec, rev, nil
}

func (b *Batch) Delete(key string, prev bool) (*Record, int64, error) {
	match := newMatcher(key)
	defer match.release()

	if elem := b.txn.Get(match); elem != nil {
		b.txn.Delete(match) // delete pair from database
		b.rev++             // update batch revision
		if !prev {
			return nil, b.rev, nil
		}
		parent := elem.(*pair)
		return newRecord(parent.last()), b.rev, nil
	}
	return nil, b.rev, errKeyNotFound
}

// Rev returns the current revision of the batch.
func (b *Batch) Rev() int64 { return b.rev }

// Next starts a new batch transaction. Only one batch transaction can
// be used at a time. Starting multiple batch transactions will cause
// the calls to block and be serialized until the current transaction
// finishes.
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
