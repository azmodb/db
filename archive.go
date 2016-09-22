package db

import (
	"github.com/azmodb/db/backend"
	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

func (db *DB) Snapshot(backend backend.Backend) (rev int64, err error) {
	db.archive.Lock()
	defer db.archive.Unlock()

	tx, err := backend.Next()
	if err != nil {
		return rev, err
	}

	var deleted []*pair
	tree := db.load()
	rev = tree.rev
	failed := tree.root.ForEach(func(elem llrb.Element) bool {
		p := elem.(*pair)
		p.mu.Lock()
		defer p.mu.Unlock()

		switch {
		case p.isDirty():
			if err = tx.Put(p.Pair, rev); err != nil {
				return true
			}
			p.state = archived
		case p.isDeleted():
			if err = tx.Delete(p.Pair); err != nil {
				return true
			}
			deleted = append(deleted, p)
		}
		return false
	})
	if failed {
		e := tx.Rollback()
		if err == nil {
			err = e
		}
		return rev, err
	}
	if err = tx.Commit(); err != nil {
		return rev, err
	}

	if len(deleted) > 0 {
		b := db.Next()
		for _, p := range deleted {
			b.txn.Delete(p)
		}
		b.Commit()
	}
	return rev, nil
}

func Open(backend backend.Backend) (*DB, error) {
	db := New()
	b := db.Next()

	if err := backend.Range(func(p *pb.Pair) error {
		pair := &pair{state: archived, Pair: p}
		b.txn.Insert(pair)
		b.rev++
		return nil
	}); err != nil {
		return nil, err
	}

	b.Commit()
	return db, nil
}
