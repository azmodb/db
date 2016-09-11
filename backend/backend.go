package backend

type RangeFunc func(key, value []byte) (done bool)

type Backend interface {
	Next(rev int64) (Batch, error)
	Range(fn RangeFunc) error
}

type Batch interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	Rollback() error
}
