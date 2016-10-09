package db

import "sync"

func queue(in <-chan Event, out chan<- Event) {
	pending := make([]Event, 0, 32) // avoid small allocations
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

type Watcher struct {
	cancel func(*Watcher)
	out    chan Event
	in     chan Event
	id     int64
}

func newWatcher(id int64, cancel func(*Watcher)) *Watcher {
	if cancel == nil {
		cancel = func(w *Watcher) {
			close(w.in)
			w.id = -1
		}
	}
	w := &Watcher{
		out:    make(chan Event, 64),
		in:     make(chan Event, 64),
		id:     id,
		cancel: cancel,
	}
	go w.run()
	return w
}

func (w *Watcher) Close() {
	if w.id <= 0 {
		return
	}
	w.cancel(w)
}

func (w *Watcher) send(ev Event) {
	if w.id <= 0 {
		return
	}
	w.in <- ev
}

func (w *Watcher) Recv() <-chan Event { return w.out }

func (w *Watcher) run() { queue(w.in, w.out) }

type stream struct {
	mu       sync.Mutex // protects watcher registry
	watchers map[int64]*Watcher
	num      int64
	running  bool
}

func (s *stream) init() {
	if s.running {
		return
	}
	s.watchers = make(map[int64]*Watcher)
	s.num = 0
	s.running = true
}

func (s *stream) Register() *Watcher {
	s.mu.Lock()
	s.init()
	s.num++
	/*
		w := &Watcher{
			out:    make(chan Event, 64),
			in:     make(chan Event, 64),
			id:     s.num,
			cancel: s.cancel,
		}
		go w.run()
	*/
	w := newWatcher(s.num, s.cancel)
	s.watchers[s.num] = w
	s.mu.Unlock()
	return w
}

func (s *stream) cancel(w *Watcher) {
	s.mu.Lock()
	if _, found := s.watchers[w.id]; found {
		delete(s.watchers, w.id)
		w.send(Event{err: watcherCanceled})
		close(w.in)
		w.id = -1
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
	for _, w := range s.watchers {
		if w.id <= 0 {
			continue
		}
		w.send(Event{
			Current: current,
			Created: b.rev,
			Data:    b.data,
		})
	}
	s.mu.Unlock()
}

func (s *stream) Close() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}

	for _, w := range s.watchers {
		delete(s.watchers, w.id)
		w.send(Event{err: pairDeleted})
		close(w.in)
		w.id = -1
	}
	s.watchers = nil
	s.num = 0
	s.running = false
	s.mu.Unlock()
}
