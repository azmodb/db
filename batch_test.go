package db

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

func TestBatchPutAndInsert(t *testing.T) {
	db := newDB(nil)
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Insert([]byte("k1"), []byte("v1.4"), false)
	b.Commit()

	want := &Record{Record: &pb.Record{Type: unicode, Current: 4}}
	values := []*pb.Value{
		&pb.Value{Unicode: []byte("v1.1"), Rev: 1},
		&pb.Value{Unicode: []byte("v1.2"), Rev: 2},
		&pb.Value{Unicode: []byte("v1.3"), Rev: 3},
		&pb.Value{Unicode: []byte("v1.4"), Rev: 4},
	}
	for _, test := range []struct {
		key    string
		rev    int64
		vers   bool
		values []*pb.Value
		err    error
	}{
		{"k1", 0, false, values[len(values)-1:], nil},
		{"k1", 0, true, values, nil},
		{"k1", -1, false, values[len(values)-1:], nil},
		{"k1", -1, true, values, nil},

		{"k1", 1, true, values[0:], nil},
		{"k1", 2, true, values[1:], nil},
		{"k1", 3, true, values[2:], nil},
		{"k1", 4, true, values[3:], nil},

		{"k1", 1, false, values[0:1], nil},
		{"k1", 2, false, values[1:2], nil},
		{"k1", 3, false, values[2:3], nil},
		{"k1", 4, false, values[3:4], nil},

		{"k1", 5, false, nil, errRevisionNotFound},
		{"k1", 5, true, nil, errRevisionNotFound},
		{"k99", 5, true, nil, errKeyNotFound},
		{"k99", 5, false, nil, errKeyNotFound},
	} {
		want.Values = test.values
		rec, err := db.Get([]byte(test.key), test.rev, test.vers)
		if err != test.err {
			t.Fatalf("insert: expected error %v, have %v", test.err, err)
		}
		if test.values == nil {
			continue
		}
		if !reflect.DeepEqual(want, rec) {
			t.Fatalf("insert: record differ:\n%+v\n%+v", want, rec)
		}
	}
}

func TestBatchPutAndInsertRecord(t *testing.T) {
	test := func(rec *Record, err error, wval interface{}, werr error, vlen int) {
		if err != werr {
			t.Fatalf("insert: expected error %v, have %v", werr, err)
		}
		if wval == nil && rec != nil {
			t.Fatalf("insert: expected <nil> record")
		}
		if wval == nil && rec == nil {
			return
		}
		if len(rec.Values) != vlen {
			t.Fatalf("insert: expected values #%d, have #%d", vlen, len(rec.Values))
		}

		last := rec.Values[0]
		switch v := wval.(type) {
		case []byte:
			if bytes.Compare(v, last.Unicode) != 0 {
				t.Fatalf("insert: expected value %q, have %q", v, last.Unicode)
			}
		case int64:
			if v != last.Numeric {
				t.Fatalf("insert: expected value %d, have %d", v, last.Numeric)
			}
		default:
			panic("unsupported data type")
		}
	}

	db := newDB(nil)

	b := db.Next() // test unicode data type
	rec, err := b.Insert([]byte("k1"), []byte("v1.1"), true)
	test(rec, err, nil, nil, 0)
	rec, err = b.Insert([]byte("k1"), []byte("v1.2"), true)
	test(rec, err, []byte("v1.1"), nil, 1)
	rec, err = b.Insert([]byte("k1"), []byte("v1.3"), false)
	test(rec, err, nil, nil, 0)
	rec, err = b.Increment([]byte("k1"), 1, true)
	test(rec, err, nil, errInvalidDataType, 0)
	rec, err = b.Decrement([]byte("k1"), 1, true)
	test(rec, err, nil, errInvalidDataType, 0)
	b.Rollback()

	b = db.Next() // test numeric data type
	rec, err = b.Increment([]byte("n1"), int64(1), true)
	test(rec, err, nil, nil, 0)
	rec, err = b.Decrement([]byte("n1"), int64(1), true)
	test(rec, err, int64(1), nil, 1)
	rec, err = b.Increment([]byte("n1"), int64(1), true)
	test(rec, err, int64(0), nil, 1)
	rec, err = b.Decrement([]byte("n1"), int64(1), false)
	test(rec, err, nil, nil, 0)
	rec, err = b.Insert([]byte("n1"), []byte("1"), true)
	test(rec, err, nil, errInvalidDataType, 0)
	rec, err = b.Put([]byte("n1"), []byte("1"), true)
	test(rec, err, nil, errInvalidDataType, 0)
	b.Rollback()

	if db.Len() != 0 || db.Rev() != 0 {
		t.Fatalf("insert: database is NOT immutable")
	}
}
