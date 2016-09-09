package db

import (
	"fmt"
	"testing"
)

func snapWalker(key []byte, val *Value, rev int64) bool {
	fmt.Printf("key = %q value: %v\n", key, val)
	return false
}

func TestBasicSnapshot(t *testing.T) {
	/*
		db := New()
		b := db.Next()
		b.Insert([]byte("k1"), []byte("v1.1"), false)
		b.Insert([]byte("k2"), []byte("v2.1"), false)

		b.Insert([]byte("k1"), []byte("v1.2"), false)
		b.Insert([]byte("k1"), []byte("v1.3"), false)
		b.Insert([]byte("k1"), []byte("v1.4"), false)
		b.Commit()

		buf := &bytes.Buffer{}
		s := db.Snapshot()

		_, err := s.WriteTo(buf)
		if err != nil {
			t.Fatalf("snapshot: write to failed: %v", err)
		}

		ndb, _, err := ReadFrom(buf)
		if err != nil {
			t.Fatalf("snapshot: read from failed: %v", err)
		}

		db.Range([]byte("k1"), []byte("k3"), 0, snapWalker)
		fmt.Println("------")
		ndb.Range([]byte("k1"), []byte("k3"), 0, snapWalker)
	*/
}
