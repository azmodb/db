package db

import "github.com/azmodb/llrb"

type Batch struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

func (b *Batch) Increment(key []byte, value int64, prev bool) (*Value, int64) {
	if p := b.numeric(key, value, false, prev); p != nil {
		data, _ := p.last()
		return newValue(data, p.revs()), b.rev
	}
	return nil, b.rev
}

func (b *Batch) Decrement(key []byte, value int64, prev bool) (*Value, int64) {
	if p := b.numeric(key, value, true, prev); p != nil {
		data, _ := p.last()
		return newValue(data, p.revs()), b.rev
	}
	return nil, b.rev
}

func (b *Batch) Insert(key []byte, value []byte, prev bool) (*Value, int64) {
	if p := b.data(key, value, false, prev); p != nil {
		data, _ := p.last()
		return newValue(data, p.revs()), b.rev
	}
	return nil, b.rev
}

func (b *Batch) Put(key []byte, value []byte, prev bool) (*Value, int64) {
	if p := b.data(key, value, true, prev); p != nil {
		data, _ := p.last()
		return newValue(data, p.revs()), b.rev
	}
	return nil, b.rev
}

func (b *Batch) numeric(key []byte, value int64, decrement, prev bool) *pair {
	match := newMatcher(key)
	defer match.Close()

	b.rev++ // increment batch revision
	var cur, last *pair
	if elem := b.txn.Get(match); elem != nil {
		last = elem.(*pair)
		cur = last.copy()
		if decrement {
			value = value * -1
		}
		cur.increment(value, b.rev)
	} else {
		cur = newPair(key, value, b.rev)
	}
	b.txn.Insert(cur)

	if !prev || last == nil {
		return nil
	}
	return last
}

func (b *Batch) data(key []byte, value []byte, ts, prev bool) *pair {
	match := newMatcher(key)
	defer match.Close()

	b.rev++ // increment batch revision
	var cur, last *pair
	if elem := b.txn.Get(match); elem != nil {
		last = elem.(*pair)
		cur = last.copy()
		if ts {
			cur.tombstone(value, b.rev)
		} else {
			cur.append(value, b.rev)
		}
	} else {
		cur = newPair(key, value, b.rev)
	}
	b.txn.Insert(cur)

	if !prev || last == nil {
		return nil
	}
	return last
}

func (b *Batch) Delete(key []byte, prev bool) (*Value, int64) {
	match := newMatcher(key)
	defer match.Close()

	if elem := b.txn.Get(match); elem != nil {
		p := elem.(*pair)
		b.rev++
		b.txn.Delete(match)

		data, _ := p.last()
		return newValue(data, p.revs()), b.rev
	}
	return nil, b.rev
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

func (b *Batch) Rev() int64 { return b.rev }
