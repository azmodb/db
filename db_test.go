package db

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

type basicResult struct {
	rec *Record
	rev int64
	err error
}

type basicStack []basicResult

func (b *basicStack) push(rec *Record, rev int64, err error) {
	(*b) = append(*b, basicResult{rec, rev, err})
}

func (b basicStack) compare(stack basicStack) error {
	if len(b) != len(stack) {
		return fmt.Errorf("stack length differ (%d:%d)", len(b), len(stack))
	}
	for i, result := range b {
		if result.rev != stack[i].rev {
			return fmt.Errorf("stack#%.4d revision differ (%d:%d)",
				i, result.rev, stack[i].rev)
		}
		if result.err != stack[i].err {
			return fmt.Errorf("stack#%.4d error differ (%v:%v)",
				i, result.err, stack[i].err)
		}
		if !reflect.DeepEqual(result.rec, stack[i].rec) {
			return fmt.Errorf("stack#%.4d record differ\n%v\n%v",
				i, result.rec, stack[i].rec)
		}
		stack[i].rec.Close()
		result.rec.Close()
	}
	return nil
}

func TestBasicInsertPutGet(t *testing.T) {
	wantResult := basicStack{
		// writable results
		/* 0000 */ basicResult{nil, 1, nil},
		/* 0001 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.1"), Revision: 1},
		}, pb.Record_Unicode}}, 2, nil},
		/* 0002 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.2"), Revision: 2},
		}, pb.Record_Unicode}}, 3, nil},
		/* 0003 */ basicResult{nil, 4, nil},
		/* 0004 */ basicResult{nil, 5, nil},

		// readonly results
		/* 0005 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.3"), Revision: 3},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0006 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.1"), Revision: 1},
			&pb.Block{Unicode: []byte("v1.2"), Revision: 2},
			&pb.Block{Unicode: []byte("v1.3"), Revision: 3},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0007 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.1"), Revision: 1},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0008 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.1"), Revision: 1},
			&pb.Block{Unicode: []byte("v1.2"), Revision: 2},
			&pb.Block{Unicode: []byte("v1.3"), Revision: 3},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0009 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.2"), Revision: 2},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0010 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.2"), Revision: 2},
			&pb.Block{Unicode: []byte("v1.3"), Revision: 3},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0011 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.3"), Revision: 3},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0012 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v1.3"), Revision: 3},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0013 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v2"), Revision: 4},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0014 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v2"), Revision: 4},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0015 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v2"), Revision: 4},
		}, pb.Record_Unicode}}, 5, nil},
		/* 0016 */ basicResult{&Record{&pb.Record{[]*pb.Block{
			&pb.Block{Unicode: []byte("v2"), Revision: 4},
		}, pb.Record_Unicode}}, 5, nil},

		/* 0017 */ basicResult{nil, 5, errRevisionNotFound},
		/* 0018 */ basicResult{nil, 5, errRevisionNotFound},

		// error results
		/* 0018 */ basicResult{nil, 5, errRevisionNotFound},
		/* 0019 */ basicResult{nil, 5, errRevisionNotFound},
		/* 0020 */ basicResult{nil, 5, errRevisionNotFound},
		/* 0021 */ basicResult{nil, 5, errRevisionNotFound},
		/* 0022 */ basicResult{nil, 5, errKeyNotFound},
		/* 0023 */ basicResult{nil, 5, errKeyNotFound},
		/* 0024 */ basicResult{nil, 5, errKeyNotFound},
		/* 0025 */ basicResult{nil, 5, errKeyNotFound},
	}

	db, result := New(), basicStack{}
	b := db.Next()
	// test writable access
	/* 0000 */ rec, rev, err := b.Insert("k1", []byte("v1.1"), true)
	result.push(rec, rev, err)
	/* 0001 */ rec, rev, err = b.Insert("k1", []byte("v1.2"), true)
	result.push(rec, rev, err)
	/* 0002 */ rec, rev, err = b.Insert("k1", []byte("v1.3"), true)
	result.push(rec, rev, err)

	/* 0003 */ rec, rev, err = b.Put("k2", []byte("v2"), true)
	result.push(rec, rev, err)
	/* 0004 */ rec, rev, err = b.Put("k3", []byte("v3"), true)
	result.push(rec, rev, err)
	b.Commit()

	// test readonly access
	/* 0005 */ rec, rev, err = db.Get("k1", 0, false)
	result.push(rec, rev, err)
	/* 0006 */ rec, rev, err = db.Get("k1", 0, true)
	result.push(rec, rev, err)

	/* 0007 */ rec, rev, err = db.Get("k1", 1, false)
	result.push(rec, rev, err)
	/* 0008 */ rec, rev, err = db.Get("k1", 1, true)
	result.push(rec, rev, err)

	/* 0009 */ rec, rev, err = db.Get("k1", 2, false)
	result.push(rec, rev, err)
	/* 0010 */ rec, rev, err = db.Get("k1", 2, true)
	result.push(rec, rev, err)

	/* 0011 */ rec, rev, err = db.Get("k1", 3, false)
	result.push(rec, rev, err)
	/* 0012 */ rec, rev, err = db.Get("k1", 3, true)
	result.push(rec, rev, err)

	/* 0013 */ rec, rev, err = db.Get("k2", 0, false)
	result.push(rec, rev, err)
	/* 0014 */ rec, rev, err = db.Get("k2", 0, true)
	result.push(rec, rev, err)

	/* 0015 */ rec, rev, err = db.Get("k2", 4, false)
	result.push(rec, rev, err)
	/* 0016 */ rec, rev, err = db.Get("k2", 4, true)
	result.push(rec, rev, err)
	/* 0017 */ rec, rev, err = db.Get("k2", 3, false)
	result.push(rec, rev, err)
	/* 0018 */ rec, rev, err = db.Get("k2", 3, true)
	result.push(rec, rev, err)

	// test revision not found error
	/* 0018 */ rec, rev, err = db.Get("k1", 4, false)
	result.push(rec, rev, err)
	/* 0019 */ rec, rev, err = db.Get("k1", 4, true)
	result.push(rec, rev, err)

	/* 0020 */ rec, rev, err = db.Get("k2", 5, false)
	result.push(rec, rev, err)
	/* 0021 */ rec, rev, err = db.Get("k2", 5, true)
	result.push(rec, rev, err)

	// test key not found error
	/* 0022 */ rec, rev, err = db.Get("k99", 0, false)
	result.push(rec, rev, err)
	/* 0023 */ rec, rev, err = db.Get("k99", 0, true)
	result.push(rec, rev, err)
	/* 0024 */ rec, rev, err = db.Get("k99", 99, false)
	result.push(rec, rev, err)
	/* 0025 */ rec, rev, err = db.Get("k99", 99, true)
	result.push(rec, rev, err)

	if err := wantResult.compare(result); err != nil {
		t.Fatal(err)
	}
}
