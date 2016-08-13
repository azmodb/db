package db

import (
	"reflect"
	"testing"
)

func TestBasicWatcher(t *testing.T) {
	db := New()
	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	donec := make(chan struct{}, 1)
	w, _ := db.Watch([]byte("k"))
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
		{value: []byte("v2"), revs: []int64{1, 2}, rev: 2},
		{value: []byte("v3"), revs: []int64{1, 2, 3}, rev: 3},
		{value: []byte("v4"), revs: []int64{1, 2, 3, 4}, rev: 4},
		{value: []byte("v5"), revs: []int64{1, 2, 3, 4, 5}, rev: 5},
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
