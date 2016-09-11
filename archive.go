package db

import (
	"github.com/azmodb/db/backend"
	"github.com/azmodb/llrb"
)

func (db *DB) Snapshot(backend backend.Backend) (rev int64, err error) {
	tree := db.load()
	rev = tree.rev
	b, err := backend.Next()
	if err != nil {
		return rev, err
	}

	failed := tree.root.ForEach(func(elem llrb.Element) bool {
		p := elem.(*pair)
		p.state.Lock()
		if p.dirty {
			var data []byte
			if data, err = p.Marshal(); err != nil {
				p.state.Unlock()
				return true
			}
			if err = b.Put(p.Key, data); err != nil {
				p.state.Unlock()
				return true
			}
		}
		p.dirty = false
		p.state.Unlock()
		return false
	})
	if failed {
		e := b.Rollback()
		if err == nil {
			err = e
		}
		return rev, err
	}
	return rev, b.Commit()
}

func (db *DB) Reload(backend backend.Backend) (rev int64, err error) {
	tree := db.load()
	rev = tree.rev
	return rev, backend.Range(func(key, value []byte) bool {
		return false
	})
}
