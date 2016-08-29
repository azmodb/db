package db

import (
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

func TestBasicWatcher(t *testing.T) {
	db := New()
	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	w, _, err := db.Watch([]byte("k"))
	if err != nil {
		t.Fatalf("basic watcher: cannot create watcher: %v", err)
	}
	defer w.Close()

	want := []*pb.Value{
		&pb.Value{Value: []byte("v2"), Revisions: []int64{1, 2}},
		&pb.Value{Value: []byte("v3"), Revisions: []int64{1, 2, 3}},
		&pb.Value{Value: []byte("v4"), Revisions: []int64{1, 2, 3, 4}},
	}
	results := make([]*pb.Value, 0, 3)
	done := make(chan struct{})
	go func() {
		for val := range w.Recv() {
			results = append(results, val)
		}
		done <- struct{}{}
	}()

	txn = db.Txn()
	txn.Put([]byte("k"), []byte("v2"), false)
	txn.Put([]byte("k"), []byte("v3"), false)
	txn.Put([]byte("k"), []byte("v4"), false)
	txn.Commit()

	w.Close()
	<-done

	if !reflect.DeepEqual(want, results) {
		t.Fatalf("basic watcher:\nexpected %v\ngot      %v", want, results)
	}
}

/*
func TestWatcherShutdown(t *testing.T) {
	db := New()

	w, rp, err := db.Watch([]byte("k"))
	if err == nil {
		t.Fatalf("shutdown: expected error, got <nil>")
	}
	if rp != 0 {
		t.Fatalf("shutdown: expected rpision 0, have %d", rp)
	}
	if w != nil {
		t.Fatalf("shutdown: expected <nil> watcher, got %v", w)
	}

	w.Close() // test close nil watcher

	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	w, rp, err = db.Watch([]byte("k"))
	if err != nil {
		t.Fatalf("shutdown: expected <nil> error, got %v", err)
	}
	if rp != 1 {
		t.Fatalf("shutdown: expected rpision 1, have %d", rp)
	}

	// send on opened watcher
	w.send(&pb.Pair{})
	w.send(&pb.Pair{})
	w.send(&pb.Pair{})

	// close multiple times
	w.Close()
	w.Close()
	w.Close()

	// send on already closed watcher
	w.send(&pb.Pair{})
	w.send(&pb.Pair{})
	w.send(&pb.Pair{})
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
	pairs := make([][]*pb.Pair, 0, watcherCount)

	for i := 0; i < watcherCount; i++ {
		w, _, _ := db.Watch([]byte("k"))
		watchers = append(watchers, w)

		ps := make([]*pb.Pair, 0, 4)
		pairs = append(pairs, ps)

		go func(w *Watcher, i int, donec chan<- struct{}) {
			for p := range w.Recv() {
				mu.Lock()
				pairs[i] = append(pairs[i], p)
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

	for i, ps := range pairs {
		if len(ps) != 4 {
			t.Fatalf("watcher #%d: expected 4 changes, got %d", i, len(ps))
		}
		for j := 2; j < 6; j++ {
			v := fmt.Sprintf("v%d", j)
			if string(ps[j-2].Value) != v {
				t.Fatalf("watcher #%d: expected value %q, got %q", i, v, ps[j-2].Value)
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
	ps := make([]*pb.Pair, 0, 4)
	go func() {
		for p := range w.Recv() {
			ps = append(ps, p)
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

	want := []*pb.Pair{
		{Value: []byte("v2"), Revisions: []int64{1, 2}, Rp: 2},
		{Value: []byte("v3"), Revisions: []int64{1, 2, 3}, Rp: 3},
		{Value: []byte("v4"), Revisions: []int64{1, 2, 3, 4}, Rp: 4},
		{Value: []byte("v5"), Revisions: []int64{1, 2, 3, 4, 5}, Rp: 5},
	}
	if !reflect.DeepEqual(want, ps) {
		t.Fatalf("basic watcher: expected pairs\n%#v\n%#v", want, ps)
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

	reg.remove([]byte("xxx"), "xxx") // remove unknown watcher
}
*/
