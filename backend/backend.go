package backend

type Backend interface {
	Range(fn func(key, value []byte) (done bool)) error

	Put(key, value []byte) error
	Delete(key []byte) error
}
