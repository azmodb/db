package db

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/azmodb/db/backend"
)

func TestEncodeDecode(t *testing.T) {
	var want = []block{
		block{Data: int64(41), Rev: 1},
		block{Data: int64(42), Rev: 2},
		block{Data: int64(43), Rev: 3},
		block{Data: int64(44), Rev: 4},
		block{Data: int64(45), Rev: 5},
		block{Data: int64(46), Rev: 6},
		block{Data: int64(47), Rev: 7},
		block{Data: int64(48), Rev: 8},
		block{Data: int64(49), Rev: 9},
		block{Data: int64(50), Rev: 10},
	}

	buf := newBuffer(nil)
	if err := encode(buf, want); err != nil {
		t.Fatalf("encode: %v", err)
	}

	got := []block{}
	if err := decode(buf, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !reflect.DeepEqual(want, got) {
		t.Fatalf("encode/decode: result differ\n%v\n%v", want, got)
	}
}

func TestBasicSnapshot(t *testing.T) {
	count := 100
	db := New()
	tx := db.Txn()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k%.3d", i))
		if _, err := tx.Put(key, i, false); err != nil {
			t.Fatalf("basic snapshot: %v", err)
		}
	}
	tx.Commit()

	b, err := backend.Open("test_backend.db", 0)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() {
		b.Close()
		os.RemoveAll("test_backend.db")
	}()

	if _, err := db.snapshot(b); err != nil {
		t.Fatalf("basic snapshot: %v", err)
	}

	ndb, err := reload(b)
	if err != nil {
		t.Fatalf("reload database: %v", err)
	}

	n, _, err := ndb.Range(nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("basic snapshot: %v", err)
	}
	defer n.Cancel()

	i := 0
	for ev := range n.Recv() {
		if ev.Err() != nil {
			break
		}
		i++
	}
	if i != count {
		t.Fatalf("basic snapshot: expected %d pairs, have %d", count, i)
	}
}
