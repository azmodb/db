package db

import (
	"fmt"
	"reflect"
	"testing"
)

func TestWatcherAddDelete(t *testing.T) {
	n := newNotifier()
	w1 := n.Add()
	w2 := n.Add()
	w3 := n.Add()
	if len(n.m) != 3 {
		t.Fatalf("add watcher: expected 3 watchers, have %d", len(n.m))
	}
	n.Delete(w1.ID())
	if len(n.m) != 2 {
		t.Fatalf("del watcher: expected 2 watchers, have %d", len(n.m))
	}
	n.Delete(w2.ID())
	if len(n.m) != 1 {
		t.Fatalf("del watcher: expected 1 watcher, have %d", len(n.m))
	}
	n.Delete(w3.ID())
	if len(n.m) != 0 {
		t.Fatalf("del watcher: expected 0 watchers, have %d", len(n.m))
	}
}

func TestNotifierClose(t *testing.T) {
	n := newNotifier()
	n.Close()
	n.Close()
	n.Close()

	n.Notify(signal{})
	n.Notify(signal{})
	n.Notify(signal{})
}

func TestBasicWatcher(t *testing.T) {
	count := 10
	db := New()
	b := db.Next()
	b.Insert("k", []byte("v0"), false)
	b.Commit()

	w, _, err := db.Watch("k")
	if err != nil {
		t.Fatalf("create watcher: %v", err)
	}
	defer w.Close()

	want := [][]byte{
		[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4"), []byte("v5"),
		[]byte("v6"), []byte("v7"), []byte("v8"), []byte("v9"), []byte("v10"),
	}
	result := make([][]byte, 0, count)
	done := make(chan struct{})
	go func(done chan<- struct{}) {
		defer close(done)

		i := 0
		for ev := range w.Recv() {
			result = append(result, ev.Blocks[0].Unicode)
			i++
			if i >= count {
				break
			}
		}
	}(done)

	for i := 1; i <= count; i++ {
		block := []byte(fmt.Sprintf("v%d", i))
		b = db.Next()
		b.Insert("k", block, false)
		b.Commit()
	}

	<-done
	if !reflect.DeepEqual(want, result) {
		t.Fatalf("watcher: invalid result\n%+v\n%+v", want, result)
	}
}
