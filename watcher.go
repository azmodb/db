package db

import "sync"

type notifier struct {
	mu sync.Mutex // protects watchers
	m  map[int]*Watcher
	id int

	notifyc chan struct{}
	donec   chan struct{}
	sigc    chan signal

	state    sync.Mutex
	shutdown bool
}

type signal struct {
	*pair
	cur int64
}

func newNotifier() *notifier {
	n := &notifier{
		m:       make(map[int]*Watcher),
		notifyc: make(chan struct{}),
		donec:   make(chan struct{}),
		sigc:    make(chan signal),
	}
	go n.run()
	return n
}

func (n *notifier) run() {
	for {
		select {
		case s := <-n.sigc:
			n.mu.Lock()
			for _, w := range n.m {
				w.send(s.last(s.cur))
			}
			n.mu.Unlock()
		case <-n.donec:
			n.notifyc <- struct{}{}
			return
		}
	}
}

func (n *notifier) Notify(p *pair, cur int64) {
	n.state.Lock()
	if n.shutdown {
		n.state.Unlock()
		return
	}

	n.sigc <- signal{pair: p, cur: cur}
	n.state.Unlock()
}

func (n *notifier) Close() {
	n.state.Lock()
	if n.shutdown {
		n.state.Unlock()
		return
	}
	n.shutdown = true
	n.donec <- struct{}{}
	<-n.notifyc
	n.state.Unlock()
}

func (n *notifier) Add() *Watcher {
	n.state.Lock()
	if n.shutdown {
		panic("appending to already closed notifier")
	}

	n.mu.Lock()
	n.id++
	w := &Watcher{
		ch: make(chan *Record),
		n:  n,
		id: n.id,
	}
	n.m[n.id] = w
	n.mu.Unlock()

	n.state.Unlock()
	return w
}

func (n *notifier) Delete(id int) {
	n.state.Lock()
	if n.shutdown {
		n.state.Unlock()
		return
	}
	delete(n.m, id)
	n.state.Unlock()
}

type Watcher struct {
	ch       chan *Record
	id       int
	n        *notifier
	state    sync.Mutex
	shutdown bool
}

func (db *DB) Watch(key []byte) (*Watcher, error) {
	match := newMatcher(key)
	defer match.Close()
	tree := db.load()

	if elem := tree.root.Get(match); elem != nil {
		db.mu.Lock()
		n, found := db.reg[string(key)]
		if !found {
			n = newNotifier()
			db.reg[string(key)] = n
		}
		watcher := n.Add()
		db.mu.Unlock()
		return watcher, nil
	}
	return nil, errKeyNotFound
}

func (db *DB) notify(p *pair, cur int64) {
	if n, found := db.reg[string(p.Key)]; found {
		n.Notify(p, cur)
	}
}

func (w *Watcher) Recv() <-chan *Record {
	return w.ch
}

func (w *Watcher) send(rec *Record) {
	w.state.Lock()
	if w.shutdown {
		w.state.Unlock()
		return
	}
	w.ch <- rec
	w.state.Unlock()
}

func (w *Watcher) ID() int { return w.id }

func (w *Watcher) Close() {
	w.state.Lock()
	if w.shutdown {
		w.state.Unlock()
		return
	}
	w.shutdown = true
	w.n.Delete(w.id)
	w.state.Unlock()
}
