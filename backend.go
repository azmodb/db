package db

import (
	"time"

	"github.com/boltdb/bolt"
)

type backend struct {
	db *bolt.DB
}

func newBackend(path string, timeout time.Duration) (*backend, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	return &backend{db: db}, nil
}

func (b *backend) Close() error { return b.db.Close() }
