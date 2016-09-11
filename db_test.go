package db

import (
	"fmt"
	"testing"
)

func TestDBIntegration(t *testing.T) {
	db := newDB(nil)
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Insert([]byte("k2"), []byte("v2.1"), false)

	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n1"), 1, false)
	b.Decrement([]byte("n2"), 1, false)
	b.Commit()

	rec, _ := db.Get([]byte("k1"), 0, true)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("k1"), 0, false)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("k1"), 1, false)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("k1"), 1, true)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("n1"), 0, true)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("n1"), 0, false)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("n1"), 7, false)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("n1"), 7, true)
	fmt.Println(rec)
}
