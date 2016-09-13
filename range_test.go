package db

import (
	"reflect"
	"testing"

	"github.com/azmodb/db/pb"
)

var (
	wantRec1 = &Record{
		Record: &pb.Record{Type: unicode, Current: 11, Values: []*pb.Value{
			&pb.Value{Unicode: []byte("v1.1"), Rev: 1},
			&pb.Value{Unicode: []byte("v1.2"), Rev: 2},
			&pb.Value{Unicode: []byte("v1.3"), Rev: 3},
		}},
	}
)

func testDefaultRange(t *testing.T, db *DB) {
	testFunc := func(i int, values []*pb.Value) RangeFunc {
		return func(key []byte, rec *Record) bool {
			if !reflect.DeepEqual(values, rec.Values) {
				t.Fatalf("range #%d: values mismatch\n%+v\n%+v", i, values, rec.Values)
			}
			rec.Close()
			return false
		}
	}

	tests := []struct {
		from, to []byte
		rev      int64
		vers     bool
		values   []*pb.Value
		fn       func(i int, values []*pb.Value) RangeFunc
	}{
		{nil, nil, -1, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, -1, true, wantRec1.Values, testFunc},
		{nil, nil, 0, false, wantRec1.Values[len(wantRec1.Values)-1:], testFunc},
		{nil, nil, 0, true, wantRec1.Values, testFunc},

		{nil, nil, 1, false, wantRec1.Values[0:1], testFunc},
		{nil, nil, 2, false, wantRec1.Values[1:2], testFunc},
		{nil, nil, 3, false, wantRec1.Values[2:3], testFunc},
		{nil, nil, 1, true, wantRec1.Values[0:], testFunc},
		{nil, nil, 2, true, wantRec1.Values[1:], testFunc},
		{nil, nil, 3, true, wantRec1.Values[2:], testFunc},

		{nil, nil, 4, false, nil, testFunc},
		{nil, nil, 4, true, nil, testFunc},
		{nil, nil, 5, false, nil, testFunc},
		{nil, nil, 5, true, nil, testFunc},
	}
	for i, test := range tests {
		db.Range(test.from, test.to, test.rev, test.vers, test.fn(i, test.values))
	}
}

func TestRevisionRange(t *testing.T) {
	db := New()
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Commit()

	testDefaultRange(t, db)
}
