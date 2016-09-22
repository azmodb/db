package db

import "github.com/azmodb/llrb"

// Batch represents a batch transaction on the database.
type Batch struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

func (b *Batch) insert(key []byte, value interface{}, ts, prev bool) (*Record, error) {
	match := newMatcher(key)
	defer match.Close()

	rev := b.rev + 1 // increment batch revision
	var p, parent *pair
	if elem := b.txn.Get(match); elem != nil {
		parent = elem.(*pair)
		p = parent.copy()

		_, isNum := value.(int64)
		if (isNum && !p.isNum()) || (!isNum && p.isNum()) {
			return nil, errIncompatibleValue
		}

		if !p.isNum() {
			if ts {
				p.tombstone(value, rev)
			} else {
				p.append(value, rev)
			}
		} else {
			p.increment(value, rev)
		}
	} else { // element does not exists
		p = newPair(key, value, rev)
	}

	b.txn.Insert(p)     // insert key/value pair
	b.db.notify(p, rev) // notify watchers
	b.rev = rev

	if !prev || parent == nil {
		return nil, nil
	}
	return parent.last(rev), nil
}

// Decrement decrements the value for a key. Returns an error if the key
// contains an unicode value.
func (b *Batch) Decrement(key []byte, value int64, prev bool) (*Record, error) {
	rec, err := b.insert(key, value*-1, false, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

// Increment increments the value for a key. Returns an error if the key
// contains an unicode value.
func (b *Batch) Increment(key []byte, value int64, prev bool) (*Record, error) {
	rec, err := b.insert(key, value, false, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

// Put sets the value for a key. If the key exists then its previous
// versions will be overwritten. Supplied key and value must not remain
// valid for the life of the batch. Returns an error if the key contains
// an unicode value.
func (b *Batch) Put(key []byte, value []byte, prev bool) (*Record, error) {
	rec, err := b.insert(key, value, true, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

// Insert inserts the value for a key. If the key exists then a new
// version will be created. Supplied key and value must not remain valid
// for the life of the batch. Returns an error if the key contains a
// numeric value.
func (b *Batch) Insert(key []byte, value []byte, prev bool) (*Record, error) {
	rec, err := b.insert(key, value, false, prev)
	if err != nil {
		rec.Close()
		return nil, err
	}
	return rec, nil
}

// Delete removes a key/value pair. If the key does not exist then
// an error is returned.
func (b *Batch) Delete(key []byte, prev bool) (*Record, error) {
	match := newMatcher(key)
	defer match.Close()

	if elem := b.txn.Get(match); elem != nil {
		p := elem.(*pair)
		b.rev++
		//b.txn.Delete(match)
		p.mu.Lock()
		p.state = deleted
		p.mu.Unlock()
		return p.last(b.rev), nil
	}
	return nil, errKeyNotFound
}

// Rev returns the current revision of the database.
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
