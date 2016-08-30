package db

import (
	"sync"

	"github.com/azmodb/db/pb"
)

type notifier struct {
	mu sync.Mutex // protects watchers
	m  map[int64]*Watcher
	id int64

	valc     chan *pb.Value
	donec    chan struct{}
	notifyc  chan struct{}
	state    sync.Mutex
	shutdown bool
}

func newNotifier() *notifier {
	n := &notifier{
		m:       make(map[int64]*Watcher),
		valc:    make(chan *pb.Value),
		donec:   make(chan struct{}),
		notifyc: make(chan struct{}),
	}
	go n.run()
	return n
}

func (n *notifier) Notify(value []byte, revs []int64) {
	n.state.Lock()
	if n.shutdown {
		n.state.Unlock()
		return
	}

	n.valc <- &pb.Value{Value: value, Revisions: revs}
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
		ch: make(chan *pb.Value),
		n:  n,
		id: n.id,
	}
	n.m[n.id] = w
	n.mu.Unlock()

	n.state.Unlock()
	return w
}

func (n *notifier) Delete(id int64) {
	n.state.Lock()
	if n.shutdown {
		n.state.Unlock()
		return
	}
	delete(n.m, id)
	n.state.Unlock()
}

func (n *notifier) run() {
	for {
		select {
		case val := <-n.valc:
			n.mu.Lock()
			for _, w := range n.m {
				w.send(val)
			}
			n.mu.Unlock()
		case <-n.donec:
			n.notifyc <- struct{}{}
			return
		}
	}
}

type Watcher struct {
	ch       chan *pb.Value
	id       int64
	n        *notifier
	state    sync.Mutex
	shutdown bool
}

func (w *Watcher) Recv() <-chan *pb.Value {
	return w.ch
}

func (w *Watcher) send(val *pb.Value) {
	w.state.Lock()
	if w.shutdown {
		w.state.Unlock()
		return
	}
	w.ch <- val
	w.state.Unlock()
}

func (w *Watcher) ID() int64 { return w.id }

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
