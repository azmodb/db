package db

import (
	"bytes"
	"testing"
)

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

func TestBatchPutAndInsert(t *testing.T) {
	db := newDB(nil)
	b := db.Next()
	b.Commit()
}
