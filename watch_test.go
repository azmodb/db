package db

import (
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
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

	n.Notify(nil, nil)
	n.Notify(nil, nil)
	n.Notify(nil, nil)
}

func TestBasicWatcher(t *testing.T) {
	db := New()
	txn := db.Txn()
	txn.Put([]byte("k"), []byte("v1"), false)
	txn.Commit()

	w1, _, err := db.Watch([]byte("k"))
	if err != nil {
		t.Fatalf("basic watcher: cannot create watcher: %v", err)
	}
	w2, _, err := db.Watch([]byte("k"))
	if err != nil {
		t.Fatalf("basic watcher: cannot create watcher: %v", err)
	}

	want := []*pb.Value{
		&pb.Value{Value: []byte("v2"), Revisions: []int64{1, 2}},
		&pb.Value{Value: []byte("v3"), Revisions: []int64{1, 2, 3}},
		&pb.Value{Value: []byte("v4"), Revisions: []int64{1, 2, 3, 4}},
	}
	results1 := make([]*pb.Value, 0, 3)
	results2 := make([]*pb.Value, 0, 3)
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			results1 = append(results1, <-w1.Recv())
		}
		done1 <- struct{}{}
	}()
	go func() {
		for i := 0; i < 3; i++ {
			results2 = append(results2, <-w2.Recv())
		}
		done2 <- struct{}{}
	}()

	txn = db.Txn()
	txn.Put([]byte("k"), []byte("v2"), false)
	txn.Put([]byte("k"), []byte("v3"), false)
	txn.Put([]byte("k"), []byte("v4"), false)
	txn.Commit()

	<-done1
	<-done2
	w1.Close()
	w2.Close()

	if !reflect.DeepEqual(want, results1) {
		t.Fatalf("basic watcher:\nexpected: %v\ngot:      %v", want, results1)
	}
	if !reflect.DeepEqual(want, results2) {
		t.Fatalf("basic watcher:\nexpected: %v\ngot:      %v", want, results2)
	}
}
