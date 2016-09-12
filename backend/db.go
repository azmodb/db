package backend

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var (
	_ Backend = (*DB)(nil) // DB must implement Batckend

	buckets    = [][]byte{metaBucket, kvBucket}
	metaBucket = []byte("meta")
	kvBucket   = []byte("kv")
)

type DB struct {
	mu    sync.Mutex // protects meta page timestamp
	stamp int64
	seq   int64

	tree *bolt.DB
}

func Open(path string, timeout time.Duration) (*DB, error) {
	tree, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if err = tree.Update(func(tx *bolt.Tx) (err error) {
		for _, name := range buckets {
			_, err = tx.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
		}
		return err
	}); err != nil {
		return nil, err
	}

	return &DB{tree: tree}, nil
}

func (db *DB) Close() error {
	if db == nil || db.tree == nil {
		return errors.New("backend database is shut down")
	}
	err := db.tree.Close()
	db.tree = nil
	db = nil
	return err
}

func (db *DB) nextMetaPage(rev int64) (key, value []byte) {
	now := time.Now().UnixNano()
	db.mu.Lock()
	switch {
	case db.stamp < now:
		db.stamp = now
		db.seq = 0
	case db.stamp > now:
		db.seq++
	default:
		db.seq++
	}
	data := make([]byte, 8+2*binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(data[0:], uint64(rev))
	n := binary.PutVarint(data[8:], db.stamp)
	n += binary.PutVarint(data[n:], db.seq)
	db.mu.Unlock()
	return data[:8], data[8 : 8+n]
}

func (db *DB) Next(rev int64) (Batch, error) {
	tx, err := db.tree.Begin(true)
	if err != nil {
		return nil, err
	}

	key, value := db.nextMetaPage(rev)
	meta := tx.Bucket(metaBucket)
	if err = meta.Put(key, value); err != nil {
		return nil, err
	}

	return &batch{
		Bucket: tx.Bucket(kvBucket),
		tx:     tx,
	}, nil
}

func (db *DB) Range(fn func(key, val []byte) bool) error {
	return db.tree.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(kvBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !fn(k, v) {
				return nil
			}
		}
		return nil
	})
}

type batch struct {
	*bolt.Bucket
	tx *bolt.Tx
}

func (b *batch) Commit() error { return b.tx.Commit() }

func (b *batch) Rollback() error { return b.tx.Rollback() }
