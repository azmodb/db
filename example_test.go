package db

import "fmt"

func ExampleBatch() {
	db := New()
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)

	b.Put([]byte("k2"), []byte("v2.1"), false)
	b.Put([]byte("k2"), []byte("v2.2"), false)
	b.Commit()

	fn := func(key []byte, rec *Record) bool {
		fmt.Printf("%s - %s\n", key, rec.Values)
		return false
	}
	db.Range(nil, nil, 0, true, fn)

	// Output:
	// k1 - [unicode:"v1.1" rev:1  unicode:"v1.2" rev:2  unicode:"v1.3" rev:3 ]
	// k2 - [unicode:"v2.2" rev:5 ]
}
