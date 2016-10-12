package db

import (
	"fmt"
	"log"
)

func Example() {
	db := New()
	tx := db.Txn()
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key%.3d", i))
		_, err := tx.Put(key, i, false)
		if err != nil {
			log.Fatalf("creating %q: %v", key, err)
		}
	}
	tx.Commit()

	n, _, err := db.Range(nil, nil, 0, 0)
	if err != nil {
		log.Fatalf("range: %v", err)
	}
	defer n.Cancel()

	for ev := range n.Recv() {
		if ev.Err() != nil {
			break
		}
		fmt.Println(string(ev.Key), ev.Data, ev.Created, ev.Current)
	}

	// Output:
	// key000 0 1 6
	// key001 1 2 6
	// key002 2 3 6
	// key003 3 4 6
	// key004 4 5 6
	// key005 5 6 6
}
