package db

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestPairAppendAndTombstone(t *testing.T) {
	test := func(p *pair, key string, value []byte, rev int64, length int) {
		last := p.blocks[len(p.blocks)-1].data.([]byte)
		if bytes.Compare(last, value) != 0 {
			t.Fatalf("pair: expected current value %q, have %q", value, last)
		}
		if p.key != key {
			t.Fatalf("pair: expected key %q, have %q", key, p.key)
		}
		if len(p.blocks) != length {
			t.Fatalf("pair: expected %d revs, have %d", length, len(p.blocks))
		}
		if p.isNum() {
			t.Fatalf("pair: expected unicode pair type, have %d", p.typ)
		}
	}

	p := newPair("k", []byte("v2"), 2)
	test(p, "k", []byte("v2"), 2, 1)

	p = p.insert([]byte("v3"), 3)
	test(p, "k", []byte("v3"), 3, 2)

	p = p.insert([]byte("v4"), 4)
	test(p, "k", []byte("v4"), 4, 3)

	p = p.tombstone([]byte("v5"), 5)
	test(p, "k", []byte("v5"), 5, 1)

	p = p.tombstone([]byte("v6"), 6)
	test(p, "k", []byte("v6"), 6, 1)
}

func TestPairIncrement(t *testing.T) {
	test := func(p *pair, key string, value, rev int64, length int) {
		last := p.blocks[len(p.blocks)-1].data.(int64)
		if last != value {
			t.Fatalf("pair: expected current value %q, have %q", value, last)
		}
		if p.key != key {
			t.Fatalf("pair: expected key %q, have %q", key, p.key)
		}
		if len(p.blocks) != length {
			t.Fatalf("pair: expected %d revs, have %d", length, len(p.blocks))
		}
		if !p.isNum() {
			t.Fatalf("pair: expected numeric pair type, have %d", p.typ)
		}
	}

	p := newPair("k", int64(1), 2)
	test(p, "k", int64(1), 2, 1)

	p = p.increment(int64(1), 3)
	test(p, "k", int64(2), 3, 1)

	p = p.increment(int64(1), 4)
	test(p, "k", int64(3), 4, 1)

	p = p.increment(int64(1)*-1, 5)
	test(p, "k", int64(2), 5, 1)

	p = p.increment(int64(2)*-1, 6)
	test(p, "k", int64(0), 6, 1)

	p = p.increment(int64(1)*-1, 7)
	test(p, "k", int64(-1), 7, 1)
}

func TestPairFindEqual(t *testing.T) {
	p := newPair("k", []byte("v2"), 2)
	p = p.insert([]byte("v3"), 3)
	p = p.insert([]byte("v4"), 4)
	p = p.insert([]byte("v10"), 10)
	p = p.insert([]byte("v11"), 11)
	p = p.insert([]byte("v12"), 12)
	p = p.insert([]byte("v13"), 13)

	for i, rev := range []int64{2, 3, 4, 10, 11, 12, 13} {
		index, found := p.find(rev, true)
		if index != i {
			t.Fatalf("find: expected index %d, have %d", i, index)
		}
		if !found {
			t.Fatalf("find: expected to find rev %d", rev)
		}
	}

	for _, rev := range []int64{-1, 0, 1, 5, 6, 7, 8, 9, 14, 99} {
		index, found := p.find(rev, true)
		if index != 0 {
			t.Fatalf("find: expected index 0, have %d", index)
		}
		if found {
			t.Fatalf("find: epxected to not find rev %d", rev)
		}
	}
}

func TestPairFindGreateThan(t *testing.T) {
	p := newPair("k", []byte("v2"), 2)
	p = p.insert([]byte("v3"), 3)
	p = p.insert([]byte("v4"), 4)
	p = p.insert([]byte("v10"), 10)
	p = p.insert([]byte("v11"), 11)
	p = p.insert([]byte("v12"), 12)
	p = p.insert([]byte("v13"), 13)

	for i, rev := range []int64{2, 3, 4, 10, 11, 12, 13} {
		index, found := p.find(rev, false)
		if index != i {
			t.Fatalf("find: expected index %d, have %d", i, index)
		}
		if !found {
			t.Fatalf("find: expected to find rev %d", rev)
		}
	}

	for _, rev := range []int64{0, -1} {
		index, found := p.find(rev, false)
		if index != 0 {
			t.Fatalf("find: expected index 0, have %d", index)
		}
		if !found {
			t.Fatalf("find: epxected to find rev %d", rev)
		}
	}

	for _, rev := range []int64{14, 99} {
		index, found := p.find(rev, false)
		if index != 0 {
			t.Fatalf("find: expected index 0, have %d", index)
		}
		if found {
			t.Fatalf("find: epxected to not find rev %d", rev)
		}
	}
}

func TestBlockMarshalUnmarshal(t *testing.T) {
	for _, src := range []block{
		block{data: []byte("hello world"), rev: math.MaxInt64},
		block{data: []byte("hello world"), rev: 42},
	} {
		srcSize := src.size()
		data := make([]byte, srcSize)
		sn := src.marshal(data)
		if sn != srcSize {
			t.Fatalf("block: expected marshal size %d, have %d", srcSize, sn)
		}

		dst := block{}
		dn := dst.unmarshal(unicode, data)
		if sn != dn {
			t.Fatalf("block: source and destination length differ")
		}
		if !reflect.DeepEqual(src, dst) {
			t.Fatalf("block: source and destination differ:\n%v\n%v",
				src, dst)
		}
	}
}
