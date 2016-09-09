package db

type Watcher struct{}

func (w *Watcher) Recv() <-chan *Value { return nil }
func (w *Watcher) ID() int64           { return 0 }
func (w *Watcher) Close()              {}

func (db *DB) Watch(key []byte) (*Watcher, int64, bool) {
	return nil, 0, false
}
