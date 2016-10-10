package db

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBasicDB(t *testing.T) {
	count := 100
	db := New()
	tx := db.Txn()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k%.3d", i))
		rev, err := tx.Put(key, i, false)
		if err != nil {
			t.Fatalf("basic: %v", err)
		}
		if rev != int64(i+1) {
			t.Fatalf("basic: expected created/current revision %d, have %d", i+1, rev)
		}
	}
	for i := 0; i < count/2; i++ {
		key := []byte(fmt.Sprintf("k%.3d", i))
		rev, err := tx.Put(key, i*2, false)
		if err != nil {
			t.Fatalf("basic: %v", err)
		}
		if rev != int64(i+count+1) {
			t.Fatalf("basic: expected created/current revision %d, have %d", i+count+1, rev)
		}
	}
	tx.Commit()

	wantCurrent := int64(count + count/2)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k%.3d", i))
		val, created, current, err := db.Get(key, 0, true)
		if err != nil {
			t.Fatalf("basic: %v", err)
		}
		if current != wantCurrent {
			t.Fatalf("basic: expected current revision %d, have %d", wantCurrent, current)
		}
		if i < count/2 {
			if val.(int) != i*2 {
				t.Fatalf("basic: expected value %d, have %d", i*2, val.(int))
			}
			if int64(i+count+1) != created {
				t.Fatalf("basic: expected created revision %d, have %d", i+count+1, created)
			}
		} else {
			if val.(int) != i {
				t.Fatalf("basic: expected value %d, have %d", i, val.(int))
			}
			if int64(i+1) != created {
				t.Fatalf("basic: expected created revision %d, have %d", i+1, created)
			}
		}
	}
}

func testForEach(t *testing.T, db *DB, rev int64, count int) {
	w, _, _ := db.Range(nil, nil, rev, 0)
	defer w.Cancel()

	i := 0
	for ev := range w.Recv() {
		if ev.Err() != nil {
			break
		}

		wantKey := []byte(fmt.Sprintf("k%.3d", i))
		if bytes.Compare(ev.Key, wantKey) != 0 {
			t.Fatalf("foreach: expected key %q, have %q", wantKey, ev.Key)
		}
		if ev.Data.(int) != i {
			t.Fatalf("foreach: expected value %d, have %d", i, ev.Data.(int))
		}
		if ev.Created != int64(i+1) {
			t.Fatalf("foreach: expected created revision %d, have %d", int64(i+1), ev.Created)
		}
		if ev.Current != int64(count) {
			t.Fatalf("foreach: expected current revision %d, have %d", count, ev.Current)
		}
		i++
	}
}

func TestForEach(t *testing.T) {
	count := 100
	db := New()
	tx := db.Txn()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k%.3d", i))
		tx.Put(key, i, false)
	}
	tx.Commit()

	testForEach(t, db, -1, count)
	testForEach(t, db, 0, count)
	testForEach(t, db, int64(count), count)
}

func TestCancelRange(t *testing.T) {
	count := 100
	db := New()
	tx := db.Txn()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k%.3d", i))
		tx.Put(key, i, false)
	}
	tx.Commit()

	w, _, _ := db.Range(nil, nil, 0, 0)
	w.Cancel()
}
