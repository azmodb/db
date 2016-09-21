package db

import (
	"bytes"

	"github.com/azmodb/llrb"
)

func rangeFunc(end []byte, rev, cur int64, vers bool, fn RangeFunc) llrb.Visitor {
	return func(elem llrb.Element) bool {
		p := elem.(*pair)
		if end != nil && bytes.Compare(p.Key, end) >= 0 {
			return true
		}

		var rec *Record
		if rev > 0 {
			index, found := p.find(rev, false)
			if !found { // revision not found
				return false
			}
			if vers {
				rec = p.from(index, cur)
			} else {
				//rec = p.at(index, cur)
				rec = p.last(cur)
			}
		} else {
			if vers {
				rec = p.from(0, cur)
			} else {
				rec = p.last(cur)
			}
		}
		return fn(p.Key, rec) // TODO: document, rangeFunc does not copy key
	}
}

// Range perform fn on all values stored in the tree over the interval
// [from, to) from left to right.
// If from is nil and to is nil it gets the keys in range [first, last].
// If from is nil and to is not nil it gets the keys in range
// [first, to].
// If from is not nil and to is not nil it gets the keys in range
// [from, to).
func (db *DB) Range(from, to []byte, rev int64, vers bool, fn RangeFunc) {
	tree := db.load()
	if from == nil && to == nil {
		tree.root.ForEach(rangeFunc(nil, rev, tree.rev, vers, fn))
		return
	}
	if from == nil && to != nil {
		tree.root.ForEach(rangeFunc(to, rev, tree.rev, vers, fn))
		return
	}

	switch cmp := bytes.Compare(from, to); {
	case cmp == 0: // invalid key sarch query range, report nothing
		return
	case cmp > 0: // invalid key sarch query range, report nothing
		return
	}

	fmatch, tmatch := newMatcher(from), newMatcher(to)
	defer func() {
		fmatch.Close()
		tmatch.Close()
	}()

	tree.root.Range(fmatch, tmatch, rangeFunc(nil, rev, tree.rev, vers, fn))
}

// RangeFunc is a function that operates on a key/value pair. If done is
// returned true, the RangeFunc is indicating that no further work needs
// to be done and so the traversal function should traverse no further.
type RangeFunc func(key []byte, rec *Record) (done bool)

// Get retrieves the value for a key at revision rev. If rev <= 0 Get
// returns the current value for a key.
func (db *DB) Get(key []byte, rev int64, vers bool) (rec *Record, err error) {
	rec, err = db.get(key, rev, vers)
	return rec, err
}

func (db *DB) get(key []byte, rev int64, vers bool) (*Record, error) {
	match := newMatcher(key)
	defer match.Close()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		var rec *Record
		if rev > 0 {
			index, found := p.find(rev, true)
			if !found {
				return nil, errRevisionNotFound
			}
			if vers {
				rec = p.from(index, tree.rev)
			} else {
				rec = p.at(index, tree.rev)
			}
		} else {
			if vers {
				rec = p.from(0, tree.rev)
			} else {
				rec = p.last(tree.rev)
			}
		}
		return rec, nil
	}
	return nil, errKeyNotFound
}
