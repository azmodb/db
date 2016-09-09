package db

import (
	"bytes"
	"reflect"
	"testing"
)

func TestDBGetStrictRevision(t *testing.T) {
	test := func(val *Value, wantVal []byte, rev, wantRev int64, wantRevs []int64) {
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
		if !reflect.DeepEqual(val.Revs(), wantRevs) {
			t.Fatalf("get: expected revisions %v, have %v", wantRevs, val.Revs())
		}
	}

	db := newDB(nil)
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Insert([]byte("k2"), []byte("xxx"), false)
	b.Insert([]byte("k1"), []byte("v1.4"), false)
	b.Insert([]byte("k1"), []byte("v1.5"), false)
	b.Commit()

	wantRevs := []int64{1, 2, 3, 5, 6}

	val, rev, _ := db.Get([]byte("k1"), 0, true)
	test(val, []byte("v1.5"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), -1, true)
	test(val, []byte("v1.5"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 1, true)
	test(val, []byte("v1.1"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 2, true)
	test(val, []byte("v1.2"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 3, true)
	test(val, []byte("v1.3"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 5, true)
	test(val, []byte("v1.4"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 6, true)
	test(val, []byte("v1.5"), rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 4, true)
	test(val, nil, rev, 6, wantRevs)

	val, rev, _ = db.Get([]byte("k1"), 7, true)
	test(val, nil, rev, 6, wantRevs)
}

func TestDBGet(t *testing.T) {
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

	db := newDB(nil)
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1"), false)
	b.Insert([]byte("k2"), []byte("v2"), false)
	b.Insert([]byte("k4"), []byte("v4"), false)
	b.Commit()

	val, rev, _ := db.Get([]byte("k1"), 0, true)
	test(val, []byte("v1"), rev, 3)

	val, rev, _ = db.Get([]byte("k2"), 0, true)
	test(val, []byte("v2"), rev, 3)

	val, rev, _ = db.Get([]byte("k4"), 0, true)
	test(val, []byte("v4"), rev, 3)

	val, rev, _ = db.Get([]byte("k0"), 0, true)
	test(val, nil, rev, 3)

	val, rev, _ = db.Get([]byte("k3"), 0, true)
	test(val, nil, rev, 3)

	val, rev, _ = db.Get([]byte("k5"), 0, true)
	test(val, nil, rev, 3)
}

func TestGetValueType(t *testing.T) {
	test := func(val *Value, wantNumeric bool, wantVal interface{}) {
		if wantVal == nil && val == nil {
			return
		}
		if wantVal == nil && val != nil {
			t.Fatalf("value: unexpected value found: %v", val)
		}

		if wantNumeric && !val.IsNum() {
			t.Fatalf("value: expected numberic value")
		}
		if val.IsNum() {
			if val.Num() != wantVal.(int64) {
				t.Fatalf("value: expected value %d, have %d", wantVal, val.Num())
			}
		} else {
			if bytes.Compare(val.Bytes(), wantVal.([]byte)) != 0 {
				t.Fatalf("value: expected value %q, have %q", wantVal, val.Bytes())
			}
		}
	}

	db := newDB(nil)
	b := db.Next()

	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k2"), []byte("v2"), false)
	b.Insert([]byte("k3"), []byte("v3"), false)
	b.Insert([]byte("k3"), []byte("v4"), false)

	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n2"), 5, false)
	b.Commit()

	val, _, _ := db.Get([]byte("k1"), 0, true)
	test(val, false, []byte("v1.2"))

	val, _, _ = db.Get([]byte("k1"), 2, true)
	test(val, false, []byte("v1.2"))

	val, _, _ = db.Get([]byte("k1"), 1, true)
	test(val, false, []byte("v1.1"))

	val, _, _ = db.Get([]byte("n1"), 0, true)
	test(val, true, int64(3))

	val, _, _ = db.Get([]byte("n2"), 0, true)
	test(val, true, int64(5))

	val, _, _ = db.Get([]byte("n1"), 7, true)
	test(val, true, nil)

	val, _, _ = db.Get([]byte("n1"), 9, true)
	test(val, true, nil)

	val, _, _ = db.Get([]byte("n1"), 8, true)
	test(val, true, int64(3))
}

/*
func TestBasicRange(t *testing.T) {
	db := newDB(nil)
	b := db.Next()
	b.Increment([]byte("k1"), 1, false)
	b.Increment([]byte("k1"), 1, false)
	b.Increment([]byte("k1"), 1, false)

	b.Insert([]byte("k2"), []byte("v2.1"), false)
	b.Insert([]byte("k2"), []byte("v2.2"), false)
	b.Insert([]byte("k2"), []byte("v2.3"), false)

	b.Insert([]byte("k3"), []byte("v3.1"), false)
	b.Commit()

	printer := func(k []byte, v *Value, rev int64) bool {
		fmt.Println(string(k), v, rev)
		return false
	}
	db.Range(nil, nil, 0, printer)
	fmt.Println("-----")
	db.Range(nil, []byte("k3"), 0, printer)
	fmt.Println("-----")
	db.Range([]byte("k3"), []byte("k1"), 0, printer)
	fmt.Println("-----")
	db.Range([]byte("k1"), []byte("k3"), 0, printer)
	fmt.Println("-----")
	//db.Range([]byte("k1"), []byte("k1"), 0, printer)
	//fmt.Println("-----")
}
*/
