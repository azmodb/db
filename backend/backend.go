package backend

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

// Backend represents a persistent key/value database. All data access is
// performed through transactions wich can be obtained through the DB.
type Backend interface {
	// Range ranges over all key/value pairs inside the database.
	Range(fn func(key, value []byte) bool) error

	// Next starts a new transaction. Only one write transaction can be
	// used at a time. Starting multiple write transactions will cause
	// the calls to block and be serialized until the current write
	// transaction finishes.
	Next(rev int64) (Txn, error)
}

// Txn represents a transaction on the database.
type Txn interface {
	// Put sets the value for a key. If the key exists then its previous
	// value will be overwritten. Supplied value must remain valid for
	// the life of the transaction. Returns a error if the key is blank,
	// if the key is too large, or if the value is too large.
	Put(key, value []byte) error

	// Delete removes a key/value pair. If the key does not exist then
	// nothing is done and a nil error is returned.
	Delete(key []byte) error

	// Commit writes all changes to disk. Commit returns an error if a
	// disk write error occures.
	Commit() error

	// Rollback closes the transaction and ignores all previous updates.
	Rollback() error
}

var (
	_ Backend = (*DB)(nil)  // DB must implement Backend
	_ Txn     = (*txn)(nil) // txn must implement Txn

	buckets    = [][]byte{metaBucket, kvBucket}
	metaBucket = []byte("meta")
	kvBucket   = []byte("kv")
)

// DB represents the default Backend implementation.
type DB struct {
	mu    sync.Mutex // protects meta page timestamp
	stamp int64
	seq   int64

	tree *bolt.DB
}

// Open creates and opens a database at the give path. If the file does
// not exist then it will be created automatically.
//
// Timeout is the amount of time to wait to obtain a file lock. When set
// to zero it will wait indefinitely. This option is only available on
// Darwin and Linux.
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

// Close releases all database resources. ALl transactions must be
// closed before closing the database.
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

// Next starts a new transaction. Only one write transaction can be used
// at a time. Starting multiple write transactions will cause the calls
// to block and be serialized until the current write transaction
// finishes.
func (db *DB) Next(rev int64) (Txn, error) {
	tx, err := db.tree.Begin(true)
	if err != nil {
		return nil, err
	}

	key, value := db.nextMetaPage(rev)
	meta := tx.Bucket(metaBucket)
	if err = meta.Put(key, value); err != nil {
		return nil, err
	}

	return &txn{
		Bucket: tx.Bucket(kvBucket),
		tx:     tx,
	}, nil
}

// Range ranges over all key/value pairs inside the database.
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

type txn struct {
	*bolt.Bucket
	tx *bolt.Tx
}

func (t *txn) Commit() error { return t.tx.Commit() }

func (t *txn) Rollback() error { return t.tx.Rollback() }
