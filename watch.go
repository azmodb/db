package db

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"
)

type Watcher struct {
	ch  chan Event
	key []byte
	id  string
	reg *registry

	mu       sync.Mutex
	shutdown bool
}

func (w *Watcher) ID() string  { return fmt.Sprintf("0x%x", w.id) }
func (w *Watcher) Key() []byte { return w.key }

func (w *Watcher) Recv() <-chan Event {
	return w.ch
}

func (w *Watcher) Close() {
	if w == nil {
		return
	}

	w.mu.Lock()
	if w.shutdown {
		w.mu.Unlock()
		return
	}

	w.reg.remove(w.key, w.id)
	w.shutdown = true
	close(w.ch)
	w.mu.Unlock()
}

func (w *Watcher) send(ev Event) {
	w.mu.Lock()
	if w.shutdown {
		w.mu.Unlock()
		return
	}

	select {
	case w.ch <- ev:
	default:
		w.shutdown = true
	}
	w.mu.Unlock()
}

type watchers map[string]*Watcher

type Event struct {
	Value []byte
	Revs  []int64
	Rev   int64
}

type registry struct {
	mu  sync.RWMutex // protects following
	w   map[string]watchers
	buf [32]byte
}

func newRegistry() *registry {
	return &registry{
		w:   make(map[string]watchers),
		buf: [32]byte{},
	}
}
func (r *registry) get(key []byte) (watchers, bool) {
	r.mu.RLock()
	watchers, found := r.w[string(key)]
	r.mu.RUnlock()
	return watchers, found
}

func (r *registry) put(key []byte) *Watcher {
	r.mu.Lock()
	watchers, found := r.w[string(key)]
	if !found {
		watchers = make(map[string]*Watcher)
		r.w[string(key)] = watchers
	}

	id := r.id()
	w := &Watcher{
		ch:  make(chan Event, 16),
		key: key,
		id:  id,
		reg: r,
	}
	watchers[id] = w
	r.mu.Unlock()
	return w
}

func (r *registry) remove(key []byte, id string) {
	r.mu.Lock()
	watchers, found := r.w[string(key)]
	if !found {
		r.mu.Unlock()
		return
	}
	delete(watchers, id)
	if len(watchers) == 0 {
		delete(r.w, string(key))
	}
	r.mu.Unlock()
}

func (r *registry) id() string {
	_, err := io.ReadFull(rand.Reader, r.buf[:])
	if err != nil {
		panic("out of entropy")
	}
	return string(r.buf[:])
}
