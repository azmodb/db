package db

import "sync"

func queue(in <-chan Event, out chan<- Event, capacity int) {
	pending := make([]Event, 0, capacity) // avoid small allocations
	defer func() {
		for _, v := range pending {
			out <- v
		}
		pending = nil
		close(out)
	}()

	for {
		if len(pending) == 0 {
			v, ok := <-in
			if !ok {
				return
			}
			pending = append(pending, v)
		}

		select {
		case v, ok := <-in:
			if !ok {
				return
			}
			pending = append(pending, v)

		case out <- pending[0]:
			pending = pending[1:]
		}
	}
}

type Event struct {
	Data    interface{}
	Created int64
	Current int64
	Key     []byte
	err     error
}

func (e Event) Err() error { return e.err }

const defaultNotifierCapacity = 64

type Notifier struct {
	cancel func(*Notifier)
	out    chan Event
	in     chan Event
	mu     sync.Mutex
	id     int64
}

func newNotifier(id int64, cancel func(*Notifier), capacity int) *Notifier {
	if id <= 0 {
		panic("watcher: cannot use id <= 0")
	}
	if cancel == nil {
		cancel = func(_ *Notifier) {}
	}
	n := &Notifier{
		out:    make(chan Event, capacity),
		in:     make(chan Event, capacity),
		id:     id,
		cancel: cancel,
	}

	if capacity > 1 {
		go queue(n.in, n.out, capacity)
	}
	return n
}

func (n *Notifier) shutdown() {
	close(n.in)
	n.id = -1
}

func (n *Notifier) Cancel() {
	n.mu.Lock()
	if n.id <= 0 {
		n.mu.Unlock()
		return
	}
	n.cancel(n)
	n.in <- Event{err: notifierCanceled}
	n.shutdown()
	n.mu.Unlock()
}

func (n *Notifier) send(key []byte, data interface{}, created, current int64) bool {
	n.mu.Lock()
	if n.id <= 0 {
		n.mu.Unlock()
		return false
	}
	n.in <- Event{
		Created: created,
		Current: current,
		Data:    data,
		Key:     key,
	}
	n.mu.Unlock()
	return true
}

func (n *Notifier) close(err error) {
	n.mu.Lock()
	if n.id <= 0 {
		n.mu.Unlock()
		return
	}
	n.in <- Event{err: err}
	n.shutdown()
	n.mu.Unlock()
}

func (w *Notifier) Recv() <-chan Event { return w.out }

type stream struct {
	mu        sync.Mutex // protects watcher registry
	notifiers map[int64]*Notifier
	num       int64
	running   bool
}

func (s *stream) init() {
	if s.running {
		return
	}
	s.notifiers = make(map[int64]*Notifier)
	s.num = 0
	s.running = true
}

func (s *stream) Register() *Notifier {
	s.mu.Lock()
	s.init()
	s.num++
	n := newNotifier(s.num, s.cancel, defaultNotifierCapacity)
	s.notifiers[s.num] = n
	s.mu.Unlock()
	return n
}

func (s *stream) cancel(n *Notifier) {
	s.mu.Lock()
	if _, found := s.notifiers[n.id]; found {
		delete(s.notifiers, n.id)
	}
	s.mu.Unlock()
}

func (s *stream) Notify(p *pair, current int64) {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}

	b := p.last()
	for _, n := range s.notifiers {
		n.send(p.key, b.data, b.rev, current)
	}
	s.mu.Unlock()
}

func (s *stream) Cancel() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}

	for _, n := range s.notifiers {
		delete(s.notifiers, n.id)
		n.close(pairDeleted)
	}
	s.notifiers = nil
	s.num = 0
	s.running = false
	s.mu.Unlock()
}
