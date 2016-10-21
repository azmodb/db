// Package backend implements the persistent AzmoDB backend key/value
// database.
package backend

import (
	"crypto/sha1"
	"errors"
	"io"
	"time"

	btree "github.com/boltdb/bolt"
	"github.com/golang/snappy"
)

// Backend represents a persistent AzmoDB backend.
type Backend interface {
	// Range performs fn on all values stored in the database at rev.
	// If fn returns an errors the traversal is stopped and the error
	// is returned.
	Range(rev Revision, fn func(key []byte, value []byte) error) error

	// Batch starts a new batch transaction. Starting multiple write
	// batch transactions will cause the calls to block and be
	// serialized until the current write batch transaction finishes.
	Batch(rev Revision) (Batch, error)

	// Last returns the last revision in the database and an error if
	// any.
	Last() (Revision, error)
}

// Batch returns a batch transaction on the database.
type Batch interface {
	// Put sets the value for a key in the database. Put must create a
	// copy of the supplied key and value.
	Put(key []byte, value []byte) error

	// Close closes the batch transaction.
	Close() error
}

// Revision represents a serialized AzmoDB revision.
type Revision [8]byte

var (
	rootBuckets = [][]byte{dataBucket, metaBucket}
	dataBucket  = []byte("__data__")
	metaBucket  = []byte("__meta__")

	_ Backend = (*DB)(nil)
)

// Option represents a DB option function.
type Option func(*DB) error

// WithMaxBatchEntries configures the maximum batch entries.
func WithMaxBatchEntries(entries int) Option {
	return func(db *DB) error {
		db.maxEntries = entries
		return nil
	}
}

// WithMaxBatchSize configures the maximum batch size.
func WithMaxBatchSize(size int) Option {
	return func(db *DB) error {
		db.maxSize = size
		return nil
	}
}

// DB represents the default AzmoDB backend. DB represents a collection of buckets
// persisted to a file on disk. All data access is performed through transactions
// which can be obtained through the DB.
type DB struct {
	root       *btree.DB
	maxEntries int
	maxSize    int
}

const (
	defaultMaxBatchSize    = 2 << 20
	defaultMaxBatchEntries = 256
)

// Open creates and opens a database at the given path. If the file does
// not exist then it will be created automatically.
//
// Timeout is the amount of time to wait to obtain a file lock. When set
// to zero it will wait indefinitely. This option is only available on
// Darwin and Linux.
func Open(path string, timeout time.Duration, opts ...Option) (*DB, error) {
	db := &DB{
		maxEntries: defaultMaxBatchEntries,
		maxSize:    defaultMaxBatchSize,
	}
	for _, opt := range opts {
		if err := opt(db); err != nil {
			return nil, err
		}
	}

	root, err := btree.Open(path, 0600, &btree.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if err = root.Update(func(tx *btree.Tx) (err error) {
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

	db.root = root
	return db, nil
}

// Close releases all database resources. All batch transactions must be
// closed before closing the database.
func (db *DB) Close() error {
	if db == nil || db.root == nil {
		return errors.New("backend is shut down")
	}

	err := db.root.Close()
	db.root = nil
	return err
}

// WriteTo writes the entire database to a writer.
func (db *DB) WriteTo(w io.Writer) (n int64, err error) {
	err = db.root.View(func(tx *btree.Tx) error {
		n, err = tx.WriteTo(w)
		return err
	})
	return n, err
}

// Range performs fn on all values stored in the database at rev. If fn
// returns an errors the traversal is stopped and the error is returned.
func (db *DB) Range(rev Revision, fn func(key, value []byte) error) error {
	return db.root.View(func(tx *btree.Tx) (err error) {
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
			safeKey := clone(nil, k)
			safeValue, err := snappy.Decode(nil, v)
			if err != nil {
				return err
			}
			if err = fn(safeKey, safeValue); err != nil {
				break
			}
		}
		return err
	})
}

// Last returns the last revision in the database and an error if any.
func (db *DB) Last() (rev Revision, err error) {
	err = db.root.View(func(tx *btree.Tx) error {
		c := tx.Bucket(metaBucket).Cursor()
		k, _ := c.Last()
		copy(rev[:], k)
		return nil
	})
	return rev, err
}

// Batch starts a new batch transaction. Starting multiple write batch
// transactions will cause the calls to block and be serialized until
// the current write batch transaction finishes.
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
		entries:    make([]*entry, db.maxEntries),
		maxEntries: db.maxEntries,
		maxSize:    db.maxSize,
		meta:       meta,
		data:       data,
		tx:         tx,
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

	meta *btree.Bucket
	data *btree.Bucket
	tx   *btree.Tx
}

func (b *batch) next() *entry {
	e := b.entries[b.index]
	if e == nil {
		e = &entry{}
		b.entries[b.index] = e
	}
	return e
}

func (b *batch) Put(key, value []byte) error {
	e := b.next()
	e.key = clone(nil, key)
	e.value = snappy.Encode(nil, value)
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
		//		b.tx.Rollback()
		return err
	}
	return b.tx.Commit()
}

func clone(dst, src []byte) []byte {
	n := len(src)
	if len(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	copy(dst, src)
	return dst
}
