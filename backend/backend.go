package backend

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/azmodb/db/pb"
	"github.com/boltdb/bolt"
)

type Backend interface {
	Range(fn func(pair *pb.Pair) error) error
	Next() (Txn, error)
	Close() error
}

type Txn interface {
	Put(pair *pb.Pair, rev int64) error
	Delete(pair *pb.Pair) error
	Commit() error
	Rollback() error
}

var (
	_ Backend = (*BoltDB)(nil) // BoltDB implements Backend
	_ Txn     = (*txn)(nil)    // txn implements Txn

	rootBuckets = [][]byte{metaBucket, kvBucket}
	metaBucket  = []byte("meta")
	kvBucket    = []byte("kv")
)

type BoltDB struct {
	db *bolt.DB
}

func Open(path string, timeout time.Duration) (*BoltDB, error) {
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

	return &BoltDB{db: db}, nil
}

func (b *BoltDB) Range(fn func(pair *pb.Pair) error) error {
	return b.db.View(func(tx *bolt.Tx) (err error) {
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

func (b *BoltDB) Next() (Txn, error) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &txn{
		meta: tx.Bucket(metaBucket),
		kv:   tx.Bucket(kvBucket),
		tx:   tx,
	}, nil
}

func (b *BoltDB) Close() error { return b.db.Close() }

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
