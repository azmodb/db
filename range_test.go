package db

import (
	"fmt"
	"testing"
)

func TestRange(t *testing.T) {
	db := New()
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)

	b.Put([]byte("k2"), []byte("v2.1"), false)
	b.Put([]byte("k2"), []byte("v2.2"), false)

	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n1"), 1, false)
	b.Increment([]byte("n1"), 1, false)

	b.Increment([]byte("n2"), 1, false)
	b.Decrement([]byte("n2"), 1, false)
	b.Decrement([]byte("n2"), 1, false)
	b.Commit()

	testFunc := func(key []byte, rec *Record) bool {
		fmt.Println(string(key), rec)
		return false
	}

	tests := []struct {
		from, to []byte
		rev      int64
		vers     bool
		fn       RangeFunc
	}{
		{nil, nil, 0, true, testFunc},
		{nil, []byte("n2"), 0, true, testFunc},
		{[]byte("k2"), nil, 0, true, testFunc},
	}
	for _, test := range tests {
		db.Range(test.from, test.to, test.rev, test.vers, test.fn)
		fmt.Println("----")
	}
}
