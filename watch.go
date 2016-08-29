package db

import (
	"sync"

	"github.com/azmodb/db/pb"
)

type notifier struct {
	m   map[int64]*Watcher // protected watcher registry
	add chan *Watcher
	del chan int64
	id  int64

	inc chan *pb.Value // value to broadcast

	done       chan struct{}
	closed     chan struct{}
	mu         sync.RWMutex // protects following
	isShutdown bool
}

func newNotifier() *notifier {
	n := &notifier{
		m:      make(map[int64]*Watcher),
		add:    make(chan *Watcher),
		del:    make(chan int64),
		inc:    make(chan *pb.Value, 1),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	go n.run()
	return n
}

// notify broadcasts the value and revisions.
func (n *notifier) Notify(value []byte, revs []int64) {
	n.inc <- &pb.Value{Value: value, Revisions: revs}
}

func (n *notifier) Shutdown() {
	n.done <- struct{}{}
	<-n.closed
}

func (n *notifier) IsShutdown() bool {
	n.mu.RLock()
	shutdown := n.isShutdown
	n.mu.RUnlock()
	return shutdown
}

func (n *notifier) run() {
	defer close(n.closed) // inform database we are done

	for {
		select {
		case val := <-n.inc:
			for _, w := range n.m {
				w.send(val)
			}
		case w := <-n.add:
			if _, found := n.m[w.id]; found {
				panic("duplicate watcher ID")
			}
			n.m[w.id] = w
		case id := <-n.del:
			delete(n.m, id)
		case <-n.done:
			for _, w := range n.m {
				w.Close()
			}
			n.mu.Lock()
			n.isShutdown = true
			n.mu.Unlock()
			return
		}
	}
}

// Create creates a new Watcher. create must be syncronized.
func (n *notifier) Create() *Watcher {
	n.id++
	w := &Watcher{
		ch: make(chan *pb.Value, 1),
		id: n.id,
		n:  n,
	}

	n.add <- w
	return w
}

func (n *notifier) remove(id int64) {
	n.del <- id
}

type Watcher struct {
	ch chan *pb.Value
	id int64

	mu       sync.Mutex
	shutdown bool

	n *notifier
}

func (w *Watcher) Recv() <-chan *pb.Value {
	return w.ch
}

func (w *Watcher) send(val *pb.Value) {
	w.mu.Lock()
	if w.shutdown || w.n.IsShutdown() {
		w.mu.Unlock()
		return
	}

	if !w.n.IsShutdown() {
		w.ch <- val
	}
	w.mu.Unlock()
}

func (w *Watcher) Close() {
	w.mu.Lock()
	if w.shutdown {
		w.mu.Unlock()
		return
	}

	w.shutdown = true
	close(w.ch)
	if !w.n.IsShutdown() {
		w.n.del <- w.id
	}
	w.mu.Unlock()
}

func (w *Watcher) ID() int64 { return w.id }
