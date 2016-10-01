package backend

import (
	"errors"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

type Backend interface {
	Range(fn func(key []byte, value []byte))
	Next() Batch
}

type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)
	Flush()
	Close()
}

type cmdType byte

const (
	putCmd cmdType = iota + 1
	delCmd
)

const maxStackSize = 256

type command struct {
	t cmdType
	k []byte
	v []byte
}

var (
	rootBuckets = [][]byte{metaBucket, kvBucket}
	metaBucket  = []byte("meta")
	kvBucket    = []byte("kv")

	_ Backend = (*DB)(nil)
)

type DB struct {
	commitc  chan []command
	tree     *bolt.DB
	writer   sync.Mutex
	mu       sync.Mutex
	shutdown bool
	closec   chan struct{}
	donec    chan struct{}
}

func Open(path string, timeout time.Duration) (*DB, error) {
	tree, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if err = tree.Update(func(tx *bolt.Tx) error {
		for _, name := range rootBuckets {
			_, err := tx.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	db := &DB{
		commitc: make(chan []command),
		tree:    tree,
		closec:  make(chan struct{}),
		donec:   make(chan struct{}),
	}
	go db.run()
	return db, nil
}

func (db *DB) Range(fn func(key, value []byte)) {
	err := db.tree.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(kvBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fn(k, v)
		}
		return nil
	})
	if err != nil { // TODO:
		panic("default backend: " + err.Error())
	}
}

func (db *DB) Next() Batch {
	db.writer.Lock()
	return &batch{db: db}
}

func (db *DB) run() {
	defer close(db.donec)

	for {
		select {
		case commands := <-db.commitc:
			err := db.tree.Update(func(tx *bolt.Tx) (err error) {
				kv := tx.Bucket(kvBucket)
				for _, cmd := range commands {
					switch cmd.t {
					case putCmd:
						err = kv.Put(cmd.k, cmd.v)
					case delCmd:
						err = kv.Delete(cmd.k)
					}
				}
				commands = nil
				return err
			})
			if err != nil { // TODO:
				panic("default backend: " + err.Error())
			}

		case <-db.closec:
			return
		}
	}
}

func (db *DB) Done() <-chan struct{} { return db.donec }

func (db *DB) Close() error {
	db.mu.Lock()
	if db.shutdown {
		db.mu.Unlock()
		return errors.New("DB is shut down")
	}
	db.shutdown = true
	close(db.closec)
	err := db.tree.Close()
	db.mu.Unlock()
	return err
}

type batch struct {
	mu    sync.Mutex
	stack []command
	db    *DB
}

func (b *batch) Put(key, value []byte) {
	b.mu.Lock()
	b.stack = append(b.stack, command{putCmd, key, value})
	if len(b.stack) >= maxStackSize {
		b.flush()
	}
	b.mu.Unlock()
}

func (b *batch) Delete(key []byte) {
	b.mu.Lock()
	b.stack = append(b.stack, command{delCmd, key, nil})
	if len(b.stack) >= maxStackSize {
		b.flush()
	}
	b.mu.Unlock()
}

func (b *batch) flush() {
	commands := b.stack
	b.db.commitc <- commands
	b.stack = nil
}

func (b *batch) Flush() {
	b.mu.Lock()
	if len(b.stack) > 0 {
		b.flush()
	}
	b.mu.Unlock()
}

func (b *batch) Close() {
	b.mu.Lock()
	if len(b.stack) > 0 {
		b.flush()
	}
	b.mu.Unlock()
	b.db.writer.Unlock()
}
