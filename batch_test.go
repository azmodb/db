package db

import (
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

func TestBatchPutAndInsert(t *testing.T) {
	db := newDB(nil)
	b := db.Next()
	b.Insert("k1", []byte("v1.1"), false)
	b.Put("k2", []byte("v2.1"), false)

	b.Insert("k1", []byte("v1.2"), false)
	b.Insert("k1", []byte("v1.3"), false)
	b.Insert("k1", []byte("v1.4"), false)

	b.Insert("k2", []byte("v2.2"), false)
	b.Insert("k2", []byte("v2.3"), false)
	b.Put("k2", []byte("v2.4"), false)
	b.Put("k2", []byte("v2.5"), false)
	b.Commit()

	want := &Record{Record: &pb.Record{Type: pb.Record_Unicode}}
	blocks1 := []*pb.Block{
		&pb.Block{Unicode: []byte("v1.1"), Revision: 1},
		&pb.Block{Unicode: []byte("v1.2"), Revision: 3},
		&pb.Block{Unicode: []byte("v1.3"), Revision: 4},
		&pb.Block{Unicode: []byte("v1.4"), Revision: 5},
	}
	/*
		blocks2 := []*pb.Block{
			&pb.Block{Unicode: []byte("v2.1"), Revision: 1},
			&pb.Block{Unicode: []byte("v2.2"), Revision: 6},
			&pb.Block{Unicode: []byte("v2.3"), Revision: 7},
			&pb.Block{Unicode: []byte("v2.4"), Revision: 8},
			&pb.Block{Unicode: []byte("v2.5"), Revision: 9},
		}
	*/

	for i, test := range []struct {
		key    string
		rev    int64
		vers   bool
		blocks []*pb.Block
		err    error
	}{
		// test get default with and without history
		{"k1", 0, false, blocks1[len(blocks1)-1:], nil},
		{"k1", 0, true, blocks1, nil},
		{"k1", -1, false, blocks1[len(blocks1)-1:], nil},
		{"k1", -1, true, blocks1, nil},

		// test get all revisions
		{"k1", 1, true, blocks1[0:], nil},
		{"k1", 3, true, blocks1[1:], nil},
		{"k1", 4, true, blocks1[2:], nil},
		{"k1", 5, true, blocks1[3:], nil},

		// test get at revision
		{"k1", 1, false, blocks1[0:1], nil},
		{"k1", 3, false, blocks1[1:2], nil},
		{"k1", 4, false, blocks1[2:3], nil},
		{"k1", 5, false, blocks1[3:4], nil},

		/*
			// test get default with and without history
			{"k2", 1, true, nil, errRevisionNotFound},
			{"k2", 1, true, nil, errRevisionNotFound},
			{"k2", 2, false, nil, errRevisionNotFound},
			{"k2", 2, true, nil, errRevisionNotFound},
			{"k2", 3, false, nil, errRevisionNotFound},
			{"k2", 3, true, nil, errRevisionNotFound},
			{"k2", 4, false, nil, errRevisionNotFound},
			{"k2", 4, true, nil, errRevisionNotFound},
			{"k2", 5, false, nil, errRevisionNotFound},
			{"k2", 5, true, nil, errRevisionNotFound},
			{"k2", 6, false, nil, errRevisionNotFound},
			{"k2", 6, false, nil, errRevisionNotFound},
			{"k2", 7, true, nil, errRevisionNotFound},
			{"k2", 7, true, nil, errRevisionNotFound},
			{"k2", 8, false, nil, errRevisionNotFound},
			{"k2", 8, true, nil, errRevisionNotFound},
			{"k2", 9, false, blocks2[len(blocks2)-1:], nil},
			{"k2", 9, true, blocks2[len(blocks2)-1:], nil},

			// test revision not found
			{"k1", 10, false, nil, errRevisionNotFound},
			{"k1", 10, true, nil, errRevisionNotFound},
			{"k1", 2, false, nil, errRevisionNotFound},
			{"k1", 2, true, nil, errRevisionNotFound},
		*/

		// test key not found
		{"k99", 5, true, nil, errKeyNotFound},
		{"k99", 5, false, nil, errKeyNotFound},
		{"k99", 0, true, nil, errKeyNotFound},
		{"k99", 0, false, nil, errKeyNotFound},
		{"k99", -1, true, nil, errKeyNotFound},
		{"k99", -1, false, nil, errKeyNotFound},
	} {
		want.Blocks = test.blocks
		rec, rev, err := db.Get(test.key, test.rev, test.vers)
		if err != test.err {
			t.Fatalf("insert #%d: expected error %v, have %v", i, test.err, err)
		}
		if test.blocks == nil {
			continue
		}
		if rev != 9 {
			t.Fatalf("insert #%d: exepected revision 9, have %d", i, rev)
		}
		if !reflect.DeepEqual(want, rec) {
			t.Fatalf("insert #%d: record differ:\n%+v\n%+v", i, want, rec)
		}
	}
}

/*
func TestBatchPutAndInsertRecord(t *testing.T) {
	test := func(rec *Record, err error, wval interface{}, werr error, vlen int) {
		if err != werr {
			t.Fatalf("insert: expected error %v, have %v", werr, err)
		}
		if wval == nil && rec != nil {
			t.Fatalf("insert: expected <nil> record")
		}
		if wval == nil && rec == nil {
			return
		}
		if len(rec.Blocks) != vlen {
			t.Fatalf("insert: expected blocks #%d, have #%d", vlen, len(rec.Blocks))
		}

		last := rec.Blocks[0]
		switch v := wval.(type) {
		case []byte:
			if bytes.Compare(v, last.Unicode) != 0 {
				t.Fatalf("insert: expected block %q, have %q", v, last.Unicode)
			}
		case int64:
			if v != last.Numeric {
				t.Fatalf("insert: expected block %d, have %d", v, last.Numeric)
			}
		default:
			panic("unsupported data type")
		}
	}

	db := newDB(nil)

	b := db.Next() // test unicode data type
	rec, err := b.Insert([]byte("k1"), []byte("v1.1"), true)
	test(rec, err, nil, nil, 0)
	rec, err = b.Insert([]byte("k1"), []byte("v1.2"), true)
	test(rec, err, []byte("v1.1"), nil, 1)
	rec, err = b.Insert([]byte("k1"), []byte("v1.3"), false)
	test(rec, err, nil, nil, 0)
	rec, err = b.Increment([]byte("k1"), 1, true)
	test(rec, err, nil, errIncompatibleBlock, 0)
	rec, err = b.Decrement([]byte("k1"), 1, true)
	test(rec, err, nil, errIncompatibleBlock, 0)
	b.Rollback()

	b = db.Next() // test numeric data type
	rec, err = b.Increment([]byte("n1"), int64(1), true)
	test(rec, err, nil, nil, 0)
	rec, err = b.Decrement([]byte("n1"), int64(1), true)
	test(rec, err, int64(1), nil, 1)
	rec, err = b.Increment([]byte("n1"), int64(1), true)
	test(rec, err, int64(0), nil, 1)
	rec, err = b.Decrement([]byte("n1"), int64(1), false)
	test(rec, err, nil, nil, 0)
	rec, err = b.Insert([]byte("n1"), []byte("1"), true)
	test(rec, err, nil, errIncompatibleBlock, 0)
	rec, err = b.Put([]byte("n1"), []byte("1"), true)
	test(rec, err, nil, errIncompatibleBlock, 0)
	b.Rollback()

	if db.Len() != 0 || db.Rev() != 0 {
		t.Fatalf("insert: database is NOT immutable")
	}
}
*/
