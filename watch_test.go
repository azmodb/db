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
