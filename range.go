package db

import (
	"bytes"

	"github.com/azmodb/llrb"
)

type RangeFunc func(key []byte, value *Value, rev int64) (done bool)

func rangeFunc(wantRev int64, fn RangeFunc, stop []byte) llrb.Visitor {
	return func(elem llrb.Element) bool {
		var data interface{}
		var rev int64
		p := elem.(*pair)

		if stop != nil {
			if bytes.Compare(p.key, stop) >= 0 {
				return true
			}
		}

		if wantRev > 0 {
			panic("TODO: range rev")
		} else {
			data, rev = p.last()
		}

		switch t := data.(type) { // we need to copy the key here
		case []byte:
			return fn(bcopy(p.key), newValue(t, p.revs()), rev)
		case int64:
			return fn(bcopy(p.key), newValue(t, p.revs()), rev)
		}
		panic("invalid value type")
	}
}

func (db *DB) Range(from, to []byte, rev int64, fn RangeFunc) int64 {
	tree := db.load()
	if from == nil && to == nil {
		tree.root.ForEach(rangeFunc(rev, fn, nil))
		return tree.rev
	}
	if from == nil && to != nil {
		tree.root.ForEach(rangeFunc(rev, fn, to))
		return tree.rev
	}

	switch cmp := bytes.Compare(from, to); {
	case cmp > 0: // invalid range, report nothing
		return tree.rev
	case cmp == 0:
		panic("TODO: get value with smaller or equal revision")
	}

	fromMatch, toMatch := newMatcher(from), newMatcher(to)
	defer func() {
		fromMatch.Close()
		toMatch.Close()
	}()

	tree.root.Range(fromMatch, toMatch, rangeFunc(rev, fn, nil))
	return tree.rev
}

func (db *DB) Get(key []byte, rev int64) (*Value, int64) {
	tree := db.load()
	match := newMatcher(key)
	defer match.Close()

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		if rev > 0 {
			data, _, found := p.find(rev)
			if !found {
				return nil, tree.rev // revision not found
			}
			return newValue(data, p.revs()), tree.rev
		}
		data, _ := p.last()
		return newValue(data, p.revs()), tree.rev
	}
	return nil, tree.rev // key not found
}
