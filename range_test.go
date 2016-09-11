package db

import (
	"fmt"
	"testing"
)

func TestDBGet(t *testing.T) {
	/*
		test := func(val *Value, wantVal []byte, rev, wantRev int64) {
			if rev != wantRev {
				t.Fatalf("get: expected revision %d, have %d", wantRev, rev)
			}
			if wantVal == nil && val != nil {
				t.Fatalf("get: expected <nil> value, have %v", val)
			}
			if wantVal == nil && val == nil {
				return
			}
			if bytes.Compare(val.Bytes(), wantVal) != 0 {
				t.Fatalf("get: expected value %q, have %q", wantVal, val)
			}
		}
	*/
	/*
		testUnicode := func(rec *Record, err error, value []byte, werr error) {
			if err != werr {
				t.Fatalf("unicode: expected error %v, have %v", werr, err)
			}
		}
	*/

	db := newDB(nil)
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k2"), []byte("v2.1"), false)
	b.Insert([]byte("k3"), []byte("v3.1"), false)
	b.Insert([]byte("k4"), []byte("v4.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Commit()

	rec, _ := db.Get([]byte("k1"), 0, true)
	fmt.Println(rec)
	rec, _ = db.Get([]byte("k1"), 0, false)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("k1"), 4, true)
	fmt.Println(rec)
	rec, _ = db.Get([]byte("k1"), 4, false)
	fmt.Println(rec)

	db.Range([]byte("k1"), []byte("k5"), 0, false, func(k []byte, r *Record) bool {
		fmt.Println(string(k), r)
		return false
	})
	/*
		val, rev, _ := db.Get([]byte("k1"), 0, false)
		test(val, []byte("v1"), rev, 3)

		val, rev, _ = db.Get([]byte("k2"), 0, false)
		test(val, []byte("v2"), rev, 3)

		val, rev, _ = db.Get([]byte("k4"), 0, false)
		test(val, []byte("v4"), rev, 3)

		val, rev, _ = db.Get([]byte("k0"), 0, false)
		test(val, nil, rev, 3)

		val, rev, _ = db.Get([]byte("k3"), 0, false)
		test(val, nil, rev, 3)

		val, rev, _ = db.Get([]byte("k5"), 0, false)
		test(val, nil, rev, 3)
	*/
}
