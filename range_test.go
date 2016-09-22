package db

import (
	"os"
	"reflect"
	"testing"

	"github.com/azmodb/db/backend"
	"github.com/azmodb/db/pb"
)

var (
	wantRec1 = &Record{
		Record: &pb.Record{Type: pb.Unicode, Current: 11, Values: []*pb.Value{
			&pb.Value{Unicode: []byte("v1.1"), Rev: 1},
			&pb.Value{Unicode: []byte("v1.2"), Rev: 2},
			&pb.Value{Unicode: []byte("v1.3"), Rev: 3},
		}},
	}
)

func testRevisionForEach(t *testing.T, db *DB) {
	testFunc := func(i int, values []*pb.Value) RangeFunc {
		return func(key []byte, rec *Record) bool {
			if !reflect.DeepEqual(values, rec.Values) {
				t.Fatalf("range #%d: values mismatch\n%+v\n%+v", i, values, rec.Values)
			}
			rec.Close()
			return false
		}
	}

	tests := []struct {
		from, to []byte
		rev      int64
		vers     bool
		values   []*pb.Value
		fn       func(i int, values []*pb.Value) RangeFunc
	}{
		// test ForEach
		{nil, nil, -1, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, -1, true, wantRec1.Values, testFunc},
		{nil, nil, 0, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, 0, true, wantRec1.Values, testFunc},

		{nil, nil, 1, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, 2, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, 3, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, 1, true, wantRec1.Values[0:], testFunc},
		{nil, nil, 2, true, wantRec1.Values[1:], testFunc},
		{nil, nil, 3, true, wantRec1.Values[2:], testFunc},

		{nil, nil, 4, false, nil, testFunc},
		{nil, nil, 4, true, nil, testFunc},
		{nil, nil, 5, false, nil, testFunc},
		{nil, nil, 5, true, nil, testFunc},

		// test ForEach with end marker
		{nil, []byte("k2"), -1, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, []byte("k2"), -1, true, wantRec1.Values, testFunc},
		{nil, []byte("k2"), 0, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, []byte("k2"), 0, true, wantRec1.Values, testFunc},

		{nil, []byte("k1"), -1, false, nil, testFunc},
		{nil, []byte("k1"), -1, true, nil, testFunc},
		{nil, []byte("k1"), 0, false, nil, testFunc},
		{nil, []byte("k1"), 0, true, nil, testFunc},

		{nil, []byte("k2"), 1, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, []byte("k2"), 2, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, []byte("k2"), 3, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, []byte("k2"), 1, true, wantRec1.Values[0:], testFunc},
		{nil, []byte("k2"), 2, true, wantRec1.Values[1:], testFunc},
		{nil, []byte("k2"), 3, true, wantRec1.Values[2:], testFunc},

		{nil, []byte("k1"), 1, false, nil, testFunc},
		{nil, []byte("k1"), 2, false, nil, testFunc},
		{nil, []byte("k1"), 3, false, nil, testFunc},
		{nil, []byte("k1"), 1, true, nil, testFunc},
		{nil, []byte("k1"), 2, true, nil, testFunc},
		{nil, []byte("k1"), 3, true, nil, testFunc},
	}
	for i, test := range tests {
		db.Range(test.from, test.to, test.rev, test.vers, test.fn(i, test.values))
	}
}

func TestRevisionRange(t *testing.T) {
	db := New()
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Commit()

	testRevisionForEach(t, db)
}

func TestDeletedGet(t *testing.T) {
	db := New()
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1"), false)
	b.Delete([]byte("k1"), false)
	b.Commit()

	if db.Len() != 1 {
		t.Fatalf("expected 1 element, have %d", db.Len())
	}

	_, err := db.Get([]byte("k1"), 0, false)
	if err != errKeyNotFound {
		t.Fatalf("expected error %v, have %v", errKeyNotFound, err)
	}

	backend, err := backend.Open("test_deleted_get.db", 0)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() {
		backend.Close()
		os.RemoveAll("test_deleted_get.db")
	}()

	if _, err = db.Snapshot(backend); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if db.Len() != 0 {
		t.Fatalf("expected 0 elements, have %d", db.Len())
	}
}
