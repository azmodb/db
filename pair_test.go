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

func TestPairFindByRevision(t *testing.T) {
	testNil := func(data interface{}, rev int64, found bool) {
		if data != nil || rev != 0 {
			t.Fatalf("pair: expected data <nil> and rev 0, have %v %d", data, rev)
		}
		if found {
			t.Fatalf("pair: should not find %v", data)
		}
	}
	test := func(data interface{}, rev int64, found bool, wantVal []byte, wantRev int64) {
		if bytes.Compare(data.([]byte), wantVal) != 0 || rev != wantRev {
			t.Fatalf("pair: expected data %v and rev %d, have %v %d",
				wantVal, wantRev, data, rev)
		}
		if !found {
			t.Fatalf("pair: should find %v", wantVal)
		}
	}

	p := newPair([]byte("k"), []byte("v1"), 10)
	p.append([]byte("v2"), 11)
	p.append([]byte("v3"), 12)
	p.append([]byte("v4"), 16)
	p.append([]byte("v5"), 17)

	for _, num := range []int64{99, 9, 13, 14, 15, 18, -1, 0} {
		data, rev, found := p.find(num, true)
		testNil(data, rev, found)
	}
	for _, num := range []int64{99, 18} {
		data, rev, found := p.find(num, true)
		testNil(data, rev, found)
	}

	data, rev, found := p.find(10, true)
	test(data, rev, found, []byte("v1"), 10)
	data, rev, found = p.find(11, true)
	test(data, rev, found, []byte("v2"), 11)
	data, rev, found = p.find(12, true)
	test(data, rev, found, []byte("v3"), 12)

	data, rev, found = p.find(1, false)
	test(data, rev, found, []byte("v5"), 17)
	data, rev, found = p.find(16, false)
	test(data, rev, found, []byte("v5"), 17)
	data, rev, found = p.find(17, false)
	test(data, rev, found, []byte("v5"), 17)
}

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
