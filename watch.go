package db

import (
	"sync"

	"github.com/azmodb/db/pb"
)

type notifier struct {
	m   map[int64]*Watcher // watcher registry
	add chan *Watcher
	del chan *Watcher
	id  int64

	inc chan *pb.Value // value to broadcast
}

func newNotifier() *notifier {
	n := &notifier{
		m:   make(map[int64]*Watcher),
		add: make(chan *Watcher),
		del: make(chan *Watcher),
		inc: make(chan *pb.Value, 1),
	}
	go n.run()
	return n
}

// notify broadcasts the value and revisions.
func (n *notifier) notify(value []byte, revs []int64) {
	n.inc <- &pb.Value{Value: value, Revisions: revs}
}

/*
func (n *notifier) shutdown() {
	n.done <- struct{}{}
	<-n.closed
}
*/

func (n *notifier) run() {
	//defer close(n.closed) // inform database we are done

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
		case w := <-n.del:
			delete(n.m, w.id)
			//case <-n.done:
			//	return
		}
	}
}

// create creates a new Watcher. create must be syncronized.
func (n *notifier) create() *Watcher {
	n.id++
	w := &Watcher{
		ch: make(chan *pb.Value, 1),
		id: n.id,
	}

	n.add <- w
	return w
}

/*
func (n *notifier) remove(w *Watcher) bool {
	select {
	case n.del <- w:
		return true
	default:
		return false
	}
}
*/

type Watcher struct {
	ch chan *pb.Value
	id int64

	mu       sync.Mutex
	shutdown bool
}

func (w *Watcher) Recv() <-chan *pb.Value {
	return w.ch
}

func (w *Watcher) send(val *pb.Value) {
	w.mu.Lock()
	if w.shutdown {
		w.mu.Unlock()
		return
	}

	w.ch <- val
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
	w.mu.Unlock()
}

func (w *Watcher) ID() int64 { return w.id }
