package db

import (
	"bytes"
	"reflect"
	"testing"
)

func TestPairAppendAndTombstone(t *testing.T) {
	test := func(p *pair, wantVal []byte, wantRev int64, wantLen int, wantRevs []int64) {
		if len(p.items) != wantLen || cap(p.items) != wantLen {
			t.Fatalf("pair: expected items length %d, have %d", wantLen, len(p.items))
		}

		data, rev := p.last()
		if bytes.Compare(data.([]byte), wantVal) != 0 {
			t.Fatalf("pair: epxected value %q, have %q", wantVal, data)
		}
		if rev != wantRev {
			t.Fatalf("pair: epxected revision %v, have %d", wantRev, rev)
		}
		if !reflect.DeepEqual(p.revs(), wantRevs) {
			t.Fatalf("pair: expected revisions %v, have %v", wantRevs, p.revs())
		}
	}

	p := newPair([]byte("k"), []byte("v1"), 10)
	test(p, []byte("v1"), 10, 1, []int64{10})

	p.append([]byte("v2"), 11)
	test(p, []byte("v2"), 11, 2, []int64{10, 11})

	p.append([]byte("v3"), 12)
	test(p, []byte("v3"), 12, 3, []int64{10, 11, 12})

	p.append([]byte("v4"), 13)
	test(p, []byte("v4"), 13, 4, []int64{10, 11, 12, 13})

	p.tombstone([]byte("v5"), 14)
	test(p, []byte("v5"), 14, 1, []int64{14})

	p.tombstone([]byte("v6"), 15)
	test(p, []byte("v6"), 15, 1, []int64{15})
}

/*
func TestPairFindByRevision(t *testing.T) {
	testNil := func(data interface{}, rev int64, found bool) {
		if data != nil || rev != 0 {
			t.Fatalf("pair: expected data <nil> and rev 0, have %v %d", data, rev)
		}
		if found {
			t.Fatalf("pair: should not find %v", data)
		}
	}
	test := func(data interface{}, rev int64, found bool, wantVal, wantRev int64) {
		if data.(int64) != wantVal || rev != wantRev {
			t.Fatalf("pair: expected data %v and rev %d, have %v %d",
				wantVal, wantRev, data, rev)
		}
		if !found {
			t.Fatalf("pair: should find %v", wantVal)
		}
	}

	p := newPair([]byte("k"), int64(101), 10)
	p.increment(int64(1), 11)
	p.increment(int64(1), 12)
	p.increment(int64(1), 16)

	data, rev, found := p.find(99)
	testNil(data, rev, found)

	data, rev, found = p.find(9)
	testNil(data, rev, found)

	data, rev, found = p.find(17)
	testNil(data, rev, found)

	data, rev, found = p.find(14)
	testNil(data, rev, found)

	data, rev, found = p.find(13)
	testNil(data, rev, found)

	data, rev, found = p.find(10)
	test(data, rev, found, 101, 10)

	data, rev, found = p.find(11)
	test(data, rev, found, 102, 11)

	data, rev, found = p.find(12)
	test(data, rev, found, 103, 12)

	data, rev, found = p.find(16)
	test(data, rev, found, 104, 16)
}
*/

func TestPairMarshalUnmarshal(t *testing.T) {
	test := []*pair{
		&pair{
			key: []byte("some key"),
			items: []item{
				item{value: []byte("data 1"), rev: 1},
				item{value: []byte("data 2"), rev: 2},
				item{value: []byte("data 3"), rev: 3},
			},
		},
		&pair{
			key: []byte("some key"),
			items: []item{
				item{value: []byte("data 1"), rev: 1},
			},
		},
	}

	for _, p1 := range test {
		buf := make([]byte, p1.size())
		n := p1.marshal(buf)
		if n != len(buf) {
			t.Fatalf("encoding: expected marshal bytes %d, have %d", len(buf), n)
		}

		p2 := &pair{}
		if err := p2.unmarshal(buf); err != nil {
			t.Fatalf("encoding: %v", err)
		}

		if !reflect.DeepEqual(p1, p2) {
			t.Fatalf("encoding: marshal/unmarshal result differ\nexpected %#v\nhave     %#v",
				p1, p2)
		}
	}
}
