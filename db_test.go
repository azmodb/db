package db

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

func rangeAll(db *DB, from, to []byte, rev int64) ([]string, int64) {
	var vals []string
	i := 0
	current := db.Range(from, to, rev, func(row *pb.Row) bool {
		vals = append(vals, string(row.Value))
		i++
		return false
	})
	return vals[:i], current
}

func getAll(db *DB, keys [][]byte, rev int64) []string {
	var vals []string
	i := 0
	for _, key := range keys {
		v, _, err := db.Get(key, rev)
		if err != nil {
			continue
		}
		vals = append(vals, string(v.Value))
		i++
	}
	return vals[:i]
}

func TestRangeGet(t *testing.T) {
	db := New()
	txn := db.Txn()
	keys := [][]byte{[]byte("k03"), []byte("k05"), []byte("k07"), []byte("k11")}
	maxKey := []byte("k12")
	vals := []string{"v3", "v5", "v7", "v11"}

	for i, key := range keys { // revision [1 4]
		txn.Put(key, []byte(vals[i]+".1"), false)
	}
	txn.Put(keys[1], []byte(vals[1]+".2"), false) // revision 5
	txn.Put(keys[2], []byte(vals[2]+".2"), false) // revision 6
	txn.Put(keys[2], []byte(vals[2]+".3"), false) // revision 7

	txn.Commit()

	tests := []struct {
		rev  int64
		want []string
	}{
		{rev: -1, want: []string{"v3.1", "v5.2", "v7.3", "v11.1"}},
		{rev: 0, want: []string{"v3.1", "v5.2", "v7.3", "v11.1"}},

		{rev: 1, want: []string{"v3.1"}},
		{rev: 2, want: []string{"v3.1", "v5.1"}},
		{rev: 3, want: []string{"v3.1", "v5.1", "v7.1"}},
		{rev: 4, want: []string{"v3.1", "v5.1", "v7.1", "v11.1"}},

		{rev: 5, want: []string{"v3.1", "v5.2", "v7.1", "v11.1"}},
		{rev: 6, want: []string{"v3.1", "v5.2", "v7.2", "v11.1"}},
		{rev: 7, want: []string{"v3.1", "v5.2", "v7.3", "v11.1"}},

		{rev: 8, want: []string{"v3.1", "v5.2", "v7.3", "v11.1"}},
		{rev: 42, want: []string{"v3.1", "v5.2", "v7.3", "v11.1"}},
	}

	for _, test := range tests {
		got, rev := rangeAll(db, keys[0], maxKey, test.rev)
		if !reflect.DeepEqual(test.want, got) {
			t.Fatalf("range at revision %d: differ\n%#v\n%#v", test.rev, test.want, got)
		}
		if rev != 7 {
			t.Fatalf("range at revision %d: expected revision %d, have %d", test.rev, rev)
		}
	}

	for _, test := range tests {
		got := getAll(db, keys, test.rev)
		if !reflect.DeepEqual(test.want, got) {
			t.Fatalf("get at revision %d: differ\n%#v\n%#v", test.rev, test.want, got)
		}
	}
}

func TestPutTombstone(t *testing.T) {
	db := New()
	txn := db.Txn()
	txn.Put([]byte("k01"), []byte("v1.1"), false) // rev = 1
	txn.Put([]byte("k01"), []byte("v1.2"), false) // rev = 2
	txn.Put([]byte("k01"), []byte("v1.3"), false) // rev = 3
	txn.Commit()

	wantVal, wantRevs, wantRev := []byte("v1.3"), []int64{1, 2, 3}, int64(3)
	v, rev, err := db.Get([]byte("k01"), 0)
	if err != nil {
		t.Fatalf("tombstone: expected to find key %q", []byte("k01"))
	}
	if !equals(v.Value, wantVal) {
		t.Fatalf("tombstone: expected value %q, got %q", wantVal, v.Value)
	}
	if !reflect.DeepEqual(v.Revisions, wantRevs) {
		t.Fatalf("tombstone: expected revisions %v, got %v", wantRevs, v.Revisions)
	}
	if rev != wantRev {
		t.Fatalf("tombstone: expected revision %d, got %d", wantRev, rev)
	}

	txn = db.Txn()
	txn.Put([]byte("k01"), []byte("v1.4"), true) // rev = 4
	txn.Commit()

	wantVal, wantRevs, wantRev = []byte("v1.4"), []int64{4}, int64(4)
	v, rev, err = db.Get([]byte("k01"), 0)
	if err != nil {
		t.Fatalf("tombstone: expected to find key %q", []byte("k01"))
	}
	if !equals(v.Value, wantVal) {
		t.Fatalf("tombstone: expected value %q, got %q", wantVal, v.Value)
	}
	if !reflect.DeepEqual(v.Revisions, wantRevs) {
		t.Fatalf("tombstone: expected revisions %v, got %v", wantRevs, v.Revisions)
	}
	if rev != wantRev {
		t.Fatalf("tombstone: expected revision %d, got %d", wantRev, rev)
	}
}

func makeValues(count, sub int) [][]byte {
	vals := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		vals = append(vals, []byte(fmt.Sprintf("val-%.4d.%d", i, sub)))
	}
	return vals
}

func makeKeys(count int) [][]byte {
	keys := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%.4d", i)))
	}
	return keys
}

func makeKey(num int) []byte {
	return []byte(fmt.Sprintf("key-%.4d", num))
}

func equals(a, b []byte) bool { return bytes.Compare(a, b) == 0 }

func TestBasicInsertedGetRangeDelete(t *testing.T) {
	count := 100
	keys, maxKey := makeKeys(count), makeKey(count+1)
	vals1, vals2, vals3 := makeValues(count, 1), makeValues(count/2, 2), makeValues(count/4, 3)

	db := New()
	txn := db.Txn()
	for i := 0; i < count; i++ {
		txn.Put(keys[i], vals1[i], false)
	}
	for i := 0; i < count/2; i++ {
		txn.Put(keys[i], vals2[i], false)
	}
	for i := 0; i < count/4; i++ {
		txn.Put(keys[i], vals3[i], false)
	}
	txn.Commit()

	wantRev := int64(count + count/2 + count/4)
	for i := 0; i < count; i++ {
		v, rev, err := db.Get(keys[i], 0)
		if err != nil {
			t.Fatalf("get: expected to find key %q", keys[i])
		}
		if rev != wantRev {
			t.Fatalf("get: revision mismatch, expected %d, got %d", wantRev, rev)
		}
		if i < count/4 {
			if !equals(v.Value, vals3[i]) {
				t.Fatalf("get: expected value[3] %q, got %q", vals3[i], v.Value)
			}
			if len(v.Revisions) != 3 {
				t.Fatalf("get: expected 3 revisions, have %d", len(v.Revisions))
			}
			continue
		}
		if i < count/2 {
			if !equals(v.Value, vals2[i]) {
				t.Fatalf("get: expected value[2] %q, got %q", vals2[i], v.Value)
			}
			if len(v.Revisions) != 2 {
				t.Fatalf("get: expected 2 revisions, have %d", len(v.Revisions))
			}
			continue
		}
		if !equals(v.Value, vals1[i]) {
			t.Fatalf("get: expected value[1] %q, got %q", vals1[i], v.Value)
		}
		if len(v.Revisions) != 1 {
			t.Fatalf("get: expected 1 revision, have %d", len(v.Revisions))
		}
	}

	i := 0
	_ = db.Range(keys[0], maxKey, 0, func(row *pb.Row) bool {
		if i < count/4 {
			if !equals(row.Value, vals3[i]) {
				t.Fatalf("get: expected value[3] %q, got %q", vals3[i], row.Value)
			}
			i++
			return false
		}
		if i < count/2 {
			if !equals(row.Value, vals2[i]) {
				t.Fatalf("get: expected value[2] %q, got %q", vals2[i], row.Value)
			}
			i++
			return false
		}

		if !equals(row.Value, vals1[i]) {
			t.Fatalf("get: expected value[1] %q, got %q", vals1[i], row.Value)
		}
		i++
		return false
	})

	txn = db.Txn()
	for i := 0; i < count/2; i++ {
		txn.Delete(keys[i])
	}
	txn.Commit()

	i = count / 2
	_ = db.Range(keys[0], maxKey, 0, func(row *pb.Row) bool {
		if !equals(row.Value, vals1[i]) {
			t.Fatalf("get: expected value[1] %q, got %q", vals1[i], row.Value)
		}
		i++
		return false
	})
}
