package backend

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

func TestDefaultDB(t *testing.T) {
	db, err := Open("test_default.db", 0)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close database: %v", err)
		}
		os.RemoveAll("test_default.db")
	}()

	txn, err := db.Next()
	if err != nil {
		t.Fatalf("next transaction: %v", err)
	}
	want := make([][]byte, 0, 100)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("k%.4d", i))
		want = append(want, key)
		p := &pb.Pair{Key: key}
		if err := txn.Put(p, int64(i)); err != nil {
			t.Fatalf("put %q:%d: %v", p.Key, i, err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit txn: %v", err)
	}

	var got [][]byte
	n := 0
	if err = db.Range(func(p *pb.Pair) error {
		got = append(got, p.Key)
		n++
		return nil
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	got = got[:n]

	if !reflect.DeepEqual(want, got) {
		t.Fatalf("range: result differ:\n%v\n%v", want, got)
	}
}
