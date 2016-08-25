package db

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestWatcherShutdown(t *testing.T) {
	db := New()

	w, rev, err := db.Watch([]byte("k"))
	if err == nil {
		t.Fatalf("shutdown: expected non <nil> error, have %v", err)
	}
	if rev != 0 {
		t.Fatalf("shutdown: expected revision 0, have %d", rev)
	}
	if w != nil {
		t.Fatalf("shutdown: expected <nil> watcher, got %v", w)
	}

	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	w, rev, err = db.Watch([]byte("k"))
	if err != nil {
		t.Fatalf("shutdown: expected <nil> error, have %v", err)
	}
	if rev != 1 {
		t.Fatalf("shutdown: expected revision 1, have %d", rev)
	}

	// send on opened watcher
	w.send(Event{})
	w.send(Event{})
	w.send(Event{})

	// close multiple times
	w.Close()
	w.Close()
	w.Close()

	// send on already closed watcher
	w.send(Event{})
	w.send(Event{})
	w.send(Event{})
}

func TestBasicWatchers(t *testing.T) {
	watcherCount := 12
	db := New()
	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	watchers := make([]*Watcher, 0, watcherCount)
	donec := make(chan struct{}, watcherCount)

	var mu sync.Mutex
	events := make([][]Event, 0, watcherCount)

	for i := 0; i < watcherCount; i++ {
		w, _, _ := db.Watch([]byte("k"))
		watchers = append(watchers, w)

		evs := make([]Event, 0, 4)
		events = append(events, evs)

		go func(w *Watcher, i int, donec chan<- struct{}) {
			for ev := range w.Recv() {
				mu.Lock()
				events[i] = append(events[i], ev)
				mu.Unlock()
			}
			donec <- struct{}{}
		}(w, i, donec)
	}

	txn = db.Txn()
	txn.Put([]byte("k"), []byte("v2"), false)
	txn.Put([]byte("k"), []byte("v3"), false)
	txn.Put([]byte("k"), []byte("v4"), false)
	txn.Put([]byte("k"), []byte("v5"), false)
	txn.Commit()

	for _, w := range watchers {
		w.Close()
	}
	i := 0
	for _ = range donec {
		i++
		if i >= watcherCount {
			break
		}
	}

	for i, evs := range events {
		if len(evs) != 4 {
			t.Fatalf("watcher #%d: expected 4 changes, got %d", i, len(evs))
		}
		for j := 2; j < 6; j++ {
			v := fmt.Sprintf("v%d", j)
			if string(evs[j-2].Value) != v {
				t.Fatalf("watcher #%d: expected value %q, got %q", i, v, evs[j-2].Value)
			}
		}
	}
}

func TestBasicWatcher(t *testing.T) {
	db := New()
	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	donec := make(chan struct{}, 1)
	w, _, _ := db.Watch([]byte("k"))
	evs := make([]Event, 0, 4)
	go func() {
		for ev := range w.Recv() {
			evs = append(evs, ev)
		}
		donec <- struct{}{}
	}()

	txn = db.Txn()
	txn.Put([]byte("k"), []byte("v2"), false)
	txn.Put([]byte("k"), []byte("v3"), false)
	txn.Put([]byte("k"), []byte("v4"), false)
	txn.Put([]byte("k"), []byte("v5"), false)
	txn.Commit()

	w.Close()
	<-donec

	want := []Event{
		{Value: []byte("v2"), Revs: []int64{1, 2}, Rev: 2},
		{Value: []byte("v3"), Revs: []int64{1, 2, 3}, Rev: 3},
		{Value: []byte("v4"), Revs: []int64{1, 2, 3, 4}, Rev: 4},
		{Value: []byte("v5"), Revs: []int64{1, 2, 3, 4, 5}, Rev: 5},
	}
	if !reflect.DeepEqual(want, evs) {
		t.Fatalf("basic watcher: expected events\n%#v\n%#v", want, evs)
	}
}

func TestBasicRegistry(t *testing.T) {
	reg := newRegistry()
	_ = reg.put([]byte("k1"))
	w2 := reg.put([]byte("k1"))
	_ = reg.put([]byte("k1"))

	w4 := reg.put([]byte("k2"))
	_ = reg.put([]byte("k3"))

	watchers1, found := reg.get([]byte("k1"))
	if !found {
		t.Fatalf("basic registry: watchers not found")
	}
	if len(watchers1) != 3 {
		t.Fatalf("basic registry: expected watchers length %d, got %d",
			3, len(watchers1))
	}

	watchers2, found := reg.get([]byte("k2"))
	if !found {
		t.Fatalf("basic registry: watchers not found")
	}
	if len(watchers2) != 1 {
		t.Fatalf("basic registry: expected watchers length %d, got %d",
			1, len(watchers2))
	}

	watchers3, found := reg.get([]byte("k3"))
	if !found {
		t.Fatalf("basic registry: watchers not found")
	}
	if len(watchers3) != 1 {
		t.Fatalf("basic registry: expected watchers length %d, got %d",
			1, len(watchers3))
	}

	reg.remove([]byte("k1"), w2.id)
	if len(watchers1) != 2 {
		t.Fatalf("basic registry: expected watchers length %d, got %d",
			2, len(watchers1))
	}

	reg.remove([]byte("k2"), w4.id)
	if len(watchers2) != 0 {
		t.Fatalf("basic registry: expected watchers length %d, got %d",
			0, len(watchers1))
	}
	if len(reg.w) != 2 {
		t.Fatalf("basic registry: expected map length %d, got %d",
			2, len(reg.w))
	}
}
