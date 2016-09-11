package db

type Watcher struct{}

func (db *DB) Watch(key []byte) (*Watcher, error) {
	return nil, nil
}

func (w *Watcher) Recv() <-chan *Record { return nil }
func (w *Watcher) ID() int64            { return 0 }
func (w *Watcher) Close()               {}
