# DB [![GoDoc](https://godoc.org/github.com/azmodb/db?status.svg)](https://godoc.org/github.com/azmodb/db) [![Build Status](https://travis-ci.org/azmodb/db.svg?branch=master)](https://travis-ci.org/azmodb/db)


Package db implements an immutable, consistent, in-memory key/value store.
DB uses an immutable Left-Leaning Red-Black tree (LLRB) internally and
supports snapshotting.

Example:

	package main

	import (
		"fmt"
		"log"

		"github.com/azmodb/db"
	)

	func main() {
		db := db.New()
		tx := db.Txn()
		for i := 0; i < 100; i++ {
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
	}
