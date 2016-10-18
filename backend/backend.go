package backend

import (
	"crypto/sha1"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

type Backend interface {
	Range(rev Revision, fn func(key []byte, value []byte)) error
	Batch(rev Revision) (Batch, error)
}

type Batch interface {
	Put(key []byte, value []byte) error
	Close() error
}

type Revision [8]byte

var (
	rootBuckets = [][]byte{dataBucket, metaBucket}
	dataBucket  = []byte("__data__")
	metaBucket  = []byte("__meta__")

	_ Backend = (*DB)(nil)
)

type Option func(*DB) error

type DB struct {
	root *bolt.DB
}

func Open(path string, timeout time.Duration, opts ...Option) (*DB, error) {
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

func (db *DB) Range(rev Revision, fn func(key, value []byte)) error {
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

func (db *DB) Batch(rev Revision) (Batch, error) {
	tx, err := db.root.Begin(true)
	if err != nil {
		return nil, err
	}

	meta, data := tx.Bucket(metaBucket), tx.Bucket(dataBucket)
	meta, err = meta.CreateBucket(rev[:])
	if err != nil {
		return nil, err
	}
	return &batch{
		entries:    make([]*entry, 12),
		maxEntries: 12,
		maxSize:    10 * 1024 * 1024,

		meta: meta,
		data: data,
		tx:   tx,
	}, nil
}

func sha1sum(data []byte) [sha1.Size]byte {
	b := [sha1.Size]byte{}
	h := sha1.New()
	h.Write(data)
	h.Sum(b[:0])
	return b
}

type entry struct {
	key   []byte
	value []byte
}

type batch struct {
	entries    []*entry
	index      int
	size       int
	maxEntries int
	maxSize    int

	meta *bolt.Bucket
	data *bolt.Bucket
	tx   *bolt.Tx
}

func (b *batch) next() *entry {
	e := b.entries[b.index]
	if e == nil {
		e = &entry{}
		b.entries[b.index] = e
	}
	return e
}

func (b *batch) Put(key, value []byte) (err error) {
	e := b.next()
	e.key = key
	e.value = value
	b.size += len(key) + len(value)
	b.index++

	return b.flush(false)
}

func (b *batch) put(key, value []byte) (err error) {
	sum := sha1sum(value)
	if v := b.data.Get(sum[:]); v == nil {
		err = b.data.Put(sum[:], value)
		if err != nil {
			return err
		}
	}
	return b.meta.Put(key, sum[:])
}

func (b *batch) flush(force bool) (err error) {
	if b.index >= b.maxEntries || b.size >= b.maxSize || force {
		for i := 0; i < b.index; i++ {
			e := b.entries[i]
			if err = b.put(e.key, e.value); err != nil {
				return err
			}
			e.key = nil
			e.value = nil
		}
		b.index = 0
		b.size = 0
	}
	return err
}

func (b *batch) Close() error {
	if err := b.flush(true); err != nil {
		return err
	}
	return b.tx.Commit()
}
