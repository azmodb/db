package backend

type Backend interface {
	Range(fn func(key, value []byte) (done bool)) error
	Next() (Batch, error)
}

type Batch interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	Rollback() error
}
