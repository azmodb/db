package db

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/azmodb/db/backend"
)

func TestBaiscSnapshot(t *testing.T) {
	count := 100
	want := make([][]byte, 0, count)
	got := make([][]byte, 0, count)

	db := New()
	b := db.Next()
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("k%.4d", i))
		v := []byte(fmt.Sprintf("v%.4d", i))
		b.Insert(k, v, false)
		want = append(want, k)
	}
	b.Commit()

	backend, err := backend.Open("test_basic.db", 0)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer func() {
		backend.Close()
		os.RemoveAll("test_basic.db")
	}()

	if _, err := db.Snapshot(backend); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	ndb, err := Open(backend)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	ndb.Range(nil, nil, 0, false, func(k []byte, rec *Record) bool {
		got = append(got, k)
		return false
	})
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("snapshot: result differ")
	}
}
