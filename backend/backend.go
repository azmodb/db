// Package backend implements the AzmoDB persistent database interface.
package backend

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/azmodb/db/pb"
	"github.com/boltdb/bolt"
)

// Backend represents a persistent key/value database. All data access is
// performed through transactions wich can be obtained through the DB.
type Backend interface {
	// Range ranges over all pairs inside the database.
	Range(fn func(pair *pb.Pair) error) error

	// Next starts a new transaction. Only one write transaction can be
	// used at a time. Starting multiple write transactions will cause
	// the calls to block and be serialized until the current write
	// transaction finishes.
	Next() (Txn, error)

	// Close releases all database resources. All transactions must be
	// closed before closing the database.
	Close() error
}

// Txn represents a transaction on the database.
type Txn interface {
	// Put sets a pair. If the pair exists then its previous value will
	// be overwritten.
	Put(pair *pb.Pair, rev int64) error

	// Delete deletes the pair. If the pair does not exist then nothing
	// is done and a nil error is returned.
	Delete(pair *pb.Pair) error

	// Commit writes all changes to disk. Commit returns an error if a
	// disk write error occures.
	Commit() error

	// Rollback closes the transaction and ignores all previous updates.
	Rollback() error
}

var (
	_ Backend = (*DB)(nil)  // DB implements Backend
	_ Txn     = (*txn)(nil) // txn implements Txn

	rootBuckets = [][]byte{metaBucket, kvBucket}
	metaBucket  = []byte("meta")
	kvBucket    = []byte("kv")
)

// DB represents the default Backend implementation.
type DB struct {
	db *bolt.DB
}

// Open creates and opens a database at the give path. If the file does
// not exist then it will be created automatically.
//
// Timeout is the amount of time to wait to obtain a file lock. When set
// to zero it will wait indefinitely. This option is only available on
// Darwin and Linux.
func Open(path string, timeout time.Duration) (*DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if err = db.Update(func(tx *bolt.Tx) (err error) {
		for _, name := range rootBuckets {
			_, err = tx.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
		}
		return err
	}); err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// Range ranges over all pairs inside the database.
func (db *DB) Range(fn func(pair *pb.Pair) error) error {
	return db.db.View(func(tx *bolt.Tx) (err error) {
		c := tx.Bucket(metaBucket).Cursor()
		kv := tx.Bucket(kvBucket)
		info := &pb.PairInfo{}
		var key [8]byte

		for k, v := c.First(); k != nil; k, v = c.Next() {
			info.Reset()
			pb.MustUnmarshal(v, info)

			binary.BigEndian.PutUint64(key[:], info.Sequence)
			data := kv.Get(key[:])
			if data == nil {
				panic(fmt.Sprintf("cannot find pair: %q", k))
			}

			pair := &pb.Pair{}
			pb.MustUnmarshal(data, pair)
			if err = fn(pair); err != nil {
				return err
			}
		}
		return err
	})
}

// Next starts a new transaction. Only one write transaction can be used
// at a time. Starting multiple write transactions will cause the calls
// to block and be serialized until the current write transaction
// finishes.
func (db *DB) Next() (Txn, error) {
	tx, err := db.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &txn{
		meta: tx.Bucket(metaBucket),
		kv:   tx.Bucket(kvBucket),
		tx:   tx,
	}, nil
}

// Close releases all database resources. All transactions must be
// closed before closing the database.
func (db *DB) Close() error { return db.db.Close() }

type txn struct {
	meta *bolt.Bucket
	kv   *bolt.Bucket
	tx   *bolt.Tx
}

func (t *txn) Put(pair *pb.Pair, rev int64) (err error) {
	now := time.Now().UnixNano()
	info := &pb.PairInfo{}

	info.Sequence, err = t.meta.NextSequence()
	if err != nil {
		return err
	}

	if data := t.meta.Get(pair.Key); data != nil {
		pb.MustUnmarshal(data, info)
	} else {
		info.CreatedRev = rev
		info.Created = now
	}
	info.ModifiedRev = rev
	info.Modified = now
	data := pb.MustMarshal(info)
	if err = t.meta.Put(pair.Key, data); err != nil {
		return err
	}

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], info.Sequence)
	return t.kv.Put(key[:], pb.MustMarshal(pair))
}

func (t *txn) Delete(pair *pb.Pair) (err error) {
	if data := t.meta.Get(pair.Key); data != nil {
		info := &pb.PairInfo{}
		pb.MustUnmarshal(data, info)

		if err = t.meta.Delete(pair.Key); err != nil {
			return err
		}

		var key [8]byte
		binary.BigEndian.PutUint64(key[:], info.Sequence)
		err = t.kv.Delete(key[:])
	}
	return err
}

func (t *txn) Commit() error   { return t.tx.Commit() }
func (t *txn) Rollback() error { return t.tx.Rollback() }
