package db

import (
	"bytes"
	"testing"
)

func TestBatchInsertAndPut(t *testing.T) {
	testBatch := func(val *Value, rev int64, wantVal interface{}, wantRev int64) {
		if rev != wantRev {
			t.Fatalf("update: expected revision %d, have %d", wantRev, rev)
		}
		if val == nil && wantVal == nil {
			return
		}
		if !val.IsNum() {
			if bytes.Compare(val.Bytes(), wantVal.([]byte)) != 0 {
				t.Fatalf("update: expected value %v, have %v", wantVal, val)
			}
		} else {
			if val.Num() != wantVal.(int64) {
				t.Fatalf("update: expected value %v, have %v", wantVal, val)
			}
		}
	}

	db := newDB(nil)
	b := db.Next()

	// [[k1 -> v1.1], [k2 -> v2.1]]
	v, rev := b.Insert([]byte("k1"), []byte("v1.1"), true)
	testBatch(v, rev, nil, 1)
	v, rev = b.Insert([]byte("k2"), []byte("v2.1"), true)
	testBatch(v, rev, nil, 2)

	// [[k1 -> v1.1,v1.2,v1.3,v1.4], [k2 -> v2.1]]
	v, rev = b.Insert([]byte("k1"), []byte("v1.2"), true)
	testBatch(v, rev, []byte("v1.1"), 3)
	v, rev = b.Insert([]byte("k1"), []byte("v1.3"), true)
	testBatch(v, rev, []byte("v1.2"), 4)
	v, rev = b.Insert([]byte("k1"), []byte("v1.4"), true)
	testBatch(v, rev, []byte("v1.3"), 5)

	// [[k1 -> v1.1,v1.2,v1.3,v1.4], [k2 -> v2.1,v2.2,v.2.3]]
	v, rev = b.Insert([]byte("k2"), []byte("v2.2"), true)
	testBatch(v, rev, []byte("v2.1"), 6)
	v, rev = b.Insert([]byte("k2"), []byte("v2.3"), true)
	testBatch(v, rev, []byte("v2.2"), 7)

	// [[k1 -> v1.1,v1.2,v1.3,v1.4], [k2 -> v.2.4]]
	v, rev = b.Put([]byte("k2"), []byte("v2.4"), true)
	testBatch(v, rev, []byte("v2.3"), 8)

	if db.Rev() != 0 {
		t.Fatalf("update: unexpected tree revisions found: %d", db.Rev())
	}
	if db.len() != 0 {
		t.Fatalf("update: unexpected tree elements found: %d", db.len())
	}
	b.Commit()
}

func TestBatchIncrementAndDecrement(t *testing.T) {
	testBatch := func(val *Value, rev int64, wantVal interface{}, wantRev int64) {
		if rev != wantRev {
			t.Fatalf("update: expected revision %d, have %d", wantRev, rev)
		}
		if val == nil && wantVal == nil {
			return
		}
		if !val.IsNum() {
			if bytes.Compare(val.Bytes(), wantVal.([]byte)) != 0 {
				t.Fatalf("update: expected value %v, have %v", wantVal, val)
			}
		} else {
			if val.Num() != wantVal.(int64) {
				t.Fatalf("update: expected value %v, have %v", wantVal, val)
			}
		}
	}

	db := newDB(nil)
	b := db.Next()

	// [[k1 -> 1], [k2 -> 1]]
	v, rev := b.Increment([]byte("k1"), 1, true)
	testBatch(v, rev, nil, 1)
	v, rev = b.Increment([]byte("k2"), 2, true)
	testBatch(v, rev, nil, 2)

	// [[k1 -> 1,3,6,10], [k2 -> 2]]
	v, rev = b.Increment([]byte("k1"), int64(2), true)
	testBatch(v, rev, int64(1), 3)
	v, rev = b.Increment([]byte("k1"), int64(3), true)
	testBatch(v, rev, int64(3), 4)
	v, rev = b.Increment([]byte("k1"), int64(4), true)
	testBatch(v, rev, int64(6), 5)

	// [[k1 -> 1,3,6,10], [k2 -> 2,4,7]]
	v, rev = b.Increment([]byte("k2"), int64(2), true)
	testBatch(v, rev, int64(2), 6)
	v, rev = b.Increment([]byte("k2"), int64(3), true)
	testBatch(v, rev, int64(4), 7)

	// [[k1 -> 1,3,6,10], [k2 -> 2,4,7,5,4]]
	v, rev = b.Decrement([]byte("k2"), int64(2), true)
	testBatch(v, rev, int64(7), 8)
	v, rev = b.Decrement([]byte("k2"), int64(1), true)
	testBatch(v, rev, int64(5), 9)

	if db.Rev() != 0 {
		t.Fatalf("update: unexpected tree revisions found: %d", db.Rev())
	}
	if db.len() != 0 {
		t.Fatalf("update: unexpected tree elements found: %d", db.len())
	}
	b.Commit()
}
