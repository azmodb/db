package backend

type Backend interface {
	Range(fn func(key, value []byte) (done bool))

	Put(key, value []byte)
	Delete(key []byte)
}
