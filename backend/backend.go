package backend

import (
	"crypto/sha1"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

type Visitor func(key []byte, value []byte)

type Backend interface {
	Range(rev [8]byte, fn Visitor) error
	Txn(rev [8]byte) (Txn, error)
}

type Txn interface {
	Put(key []byte, value []byte) error
	Commit() error
	Rollback() error
}

var (
	rootBuckets = [][]byte{dataBucket, metaBucket}
	dataBucket  = []byte("__data__")
	metaBucket  = []byte("__meta__")

	_ Backend = (*DB)(nil)
)

type DB struct {
	root *bolt.DB
}

func Open(path string, timeout time.Duration) (*DB, error) {
	root, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if err = root.Update(func(tx *bolt.Tx) (err error) {
		for _, name := range rootBuckets {
			_, err = tx.CreateBucketIfNotExists(name)
			if err != nil {
				break
			}
		}
		return err
	}); err != nil {
		return nil, err
	}

	return &DB{root: root}, nil
}

func (db *DB) Close() error {
	if db == nil || db.root == nil {
		return errors.New("backend is shut down")
	}

	err := db.root.Close()
	db.root = nil
	return err
}

func (db *DB) Range(rev [8]byte, fn Visitor) error {
	return db.root.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket).Bucket(rev[:])
		if meta == nil {
			return errors.New("revision not found")
		}
		data := tx.Bucket(dataBucket)

		c := meta.Cursor()
		for k, sum := c.First(); k != nil; k, sum = c.Next() {
			v := data.Get(sum)
			if v == nil {
				panic("cannot find value for key: " + string(k))
			}
			fn(k, v)
		}
		return nil
	})
}

func (db *DB) Txn(rev [8]byte) (Txn, error) {
	tx, err := db.root.Begin(true)
	if err != nil {
		return nil, err
	}

	meta, data := tx.Bucket(metaBucket), tx.Bucket(dataBucket)
	meta, err = meta.CreateBucket(rev[:])
	if err != nil {
		return nil, err
	}
	return &txn{meta: meta, data: data, Tx: tx}, nil
}

func sha1sum(data []byte) [sha1.Size]byte {
	b := [sha1.Size]byte{}
	h := sha1.New()
	h.Write(data)
	h.Sum(b[:0])
	return b
}

type txn struct {
	meta *bolt.Bucket
	data *bolt.Bucket
	*bolt.Tx
}

func (tx *txn) Put(key, value []byte) (err error) {
	sum := sha1sum(value)
	if v := tx.data.Get(sum[:]); v == nil {
		err = tx.data.Put(sum[:], value)
		if err != nil {
			return err
		}
	}
	return tx.meta.Put(key, sum[:])
}
