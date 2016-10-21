package backend

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	btree "github.com/boltdb/bolt"
)

func testDefaultBackend(t *testing.T, db *DB) {
	rev := [8]byte{}
	binary.BigEndian.PutUint64(rev[:], 42)

	batch, err := db.Batch(rev)
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	count := 100
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("k%.3d", i))
		v := []byte(fmt.Sprintf("v%.3d", i))
		if err := batch.Put(k, v); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	i := 0
	if err := db.Range(rev, func(key, val []byte) error {
		k := []byte(fmt.Sprintf("k%.3d", i))
		v := []byte(fmt.Sprintf("v%.3d", i))
		if bytes.Compare(k, key) != 0 {
			t.Fatalf("range: exptected key %q, got %q", k, key)
		}
		if bytes.Compare(v, val) != 0 {
			t.Fatalf("range: exptected key %q, got %q", v, val)
		}
		i++
		return nil
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	if i != count {
		t.Fatalf("range: expected count %d, got %d", count, i)
	}
}

func TestDefaultBackend(t *testing.T) {
	db, err := Open("test_default_backend.db", 0)
	if err != nil {
		t.Fatalf("open default database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close default database: %v", err)
		}
		os.RemoveAll("test_default_backend.db")
	}()

	testDefaultBackend(t, db)
}

func insertEntries(t *testing.T, count int, r uint64, db *DB) {
	rev := [8]byte{}
	binary.BigEndian.PutUint64(rev[:], r)

	batch, err := db.Batch(rev)
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("k%.3d", i))
		v := []byte(fmt.Sprintf("v%.3d", i))
		if err := batch.Put(k, v); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestDuplicateValues(t *testing.T) {
	db, err := Open("test_duplicate_entries.db", 0)
	if err != nil {
		t.Fatalf("open default database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test_duplicate_entries.db")
	}()

	count := 10
	insertEntries(t, count, 1, db)
	insertEntries(t, count, 2, db)

	i := 0
	err = db.root.View(func(tx *btree.Tx) error {
		data := tx.Bucket(dataBucket)
		c := data.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("range data bucket: %v", err)
	}

	if i != count {
		t.Fatalf("backend: expected %d entries, have %d", count, i)
	}
}
