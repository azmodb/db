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
	current int64
	p       *pair
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
		case sig := <-n.sigc:
			n.mu.Lock()
			for _, w := range n.m {
				w.send(newEvent(sig.current, sig.p.last()))
			}
			n.mu.Unlock()
		case <-n.donec:
			n.notifyc <- struct{}{}
			return
		}
	}
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

func (n *notifier) Notify(sig signal) {
	n.state.Lock()
	if n.shutdown {
		n.state.Unlock()
		return
	}

	n.sigc <- sig
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
		ch: make(chan *Event),
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
	ch       chan *Event
	id       int
	n        *notifier
	state    sync.Mutex
	shutdown bool
}

func (w *Watcher) Recv() <-chan *Event { return w.ch }

func (db *DB) Watch(key string) (*Watcher, int64, error) {
	match := newMatcher(key)
	defer match.release()
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
		return watcher, tree.rev, nil
	}
	return nil, tree.rev, errKeyNotFound
}

func (db *DB) notify(p *pair, current int64) {
	if n, found := db.reg[p.key]; found {
		n.Notify(signal{current: current, p: p})
	}
}

func (w *Watcher) send(ev *Event) {
	w.state.Lock()
	if w.shutdown {
		w.state.Unlock()
		return
	}
	w.ch <- ev
	w.state.Unlock()
}

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

func (w *Watcher) ID() int { return w.id }
