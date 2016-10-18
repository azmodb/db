package backend

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestDefaultBackend(t *testing.T) {
	db, err := Open("test_default_backend.db", 0)
	if err != nil {
		t.Fatalf("open default database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test_default_backend.db")
	}()

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
	if err := db.Range(rev, func(key, val []byte) {
		k := []byte(fmt.Sprintf("k%.3d", i))
		v := []byte(fmt.Sprintf("v%.3d", i))
		if bytes.Compare(k, key) != 0 {
			t.Fatalf("range: exptected key %q, got %q", k, key)
		}
		if bytes.Compare(v, val) != 0 {
			t.Fatalf("range: exptected key %q, got %q", v, val)
		}
		i++
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	if i != count {
		t.Fatalf("range: expected count %d, got %d", count, i)
	}
}
