package db

import (
	"errors"

	"github.com/azmodb/llrb"
)

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
			index, found := p.find(rev)
			if !found {
				return nil, errors.New("revision not found")
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
	return nil, errors.New("key not found")

}

func (db *DB) Range(from, to []byte, rev int64, vers bool, fn RangeFunc) {
	panic("not implemented")
}

func rangeFunc() llrb.Visitor {
	return func(elem llrb.Element) bool {
		//p := elem.(*pair)

		return false
	}
}

type RangeFunc func(key []byte, rec *Record) (done bool)
