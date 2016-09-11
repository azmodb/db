package db

import (
	"bytes"
	"testing"
)

func TestPairAppendAndTombstone(t *testing.T) {
	test := func(p *pair, wkey, wval []byte, wrev int64, vlen int) {
		last := p.Values[len(p.Values)-1]
		if bytes.Compare(last.Unicode, wval) != 0 {
			t.Fatalf("pair: expected current value %q, have %q", wval, last.Unicode)
		}
		if bytes.Compare(p.Key, wkey) != 0 {
			t.Fatalf("pair: expected key %q, have %q", wkey, p.Key)
		}
		if len(p.Values) != vlen {
			t.Fatalf("pair: expected %d revs, have %d", vlen, len(p.Values))
		}
		if p.isNum() {
			t.Fatalf("pair: expected unicode pair type, have %d", p.Type)
		}
	}

	p := newPair([]byte("k"), []byte("v2"), 2)
	test(p, []byte("k"), []byte("v2"), 2, 1)

	p.append([]byte("v3"), 3)
	test(p, []byte("k"), []byte("v3"), 3, 2)

	p.append([]byte("v4"), 4)
	test(p, []byte("k"), []byte("v4"), 4, 3)

	p.tombstone([]byte("v5"), 5)
	test(p, []byte("k"), []byte("v5"), 5, 1)

	p.tombstone([]byte("v6"), 6)
	test(p, []byte("k"), []byte("v6"), 6, 1)
}

func TestPairIncrement(t *testing.T) {
	test := func(p *pair, wkey []byte, wval, wrev int64, vlen int) {
		last := p.Values[len(p.Values)-1]
		if last.Numeric != wval {
			t.Fatalf("pair: expected current value %q, have %q", wval, last.Numeric)
		}
		if bytes.Compare(p.Key, wkey) != 0 {
			t.Fatalf("pair: expected key %q, have %q", wkey, p.Key)
		}
		if len(p.Values) != vlen {
			t.Fatalf("pair: expected %d revs, have %d", vlen, len(p.Values))
		}
		if !p.isNum() {
			t.Fatalf("pair: expected numeric pair type, have %d", p.Type)
		}
	}

	p := newPair([]byte("k"), int64(1), 2)
	test(p, []byte("k"), int64(1), 2, 1)

	p.increment(int64(1), 3)
	test(p, []byte("k"), int64(2), 3, 1)

	p.increment(int64(1), 4)
	test(p, []byte("k"), int64(3), 4, 1)

	p.increment(int64(1)*-1, 5)
	test(p, []byte("k"), int64(2), 5, 1)

	p.increment(int64(2)*-1, 6)
	test(p, []byte("k"), int64(0), 6, 1)

	p.increment(int64(1)*-1, 7)
	test(p, []byte("k"), int64(-1), 7, 1)
}

func TestPairFindMustEqual(t *testing.T) {
	p := newPair([]byte("k"), []byte("v2"), 2)
	p.append([]byte("v3"), 3)
	p.append([]byte("v4"), 4)
	p.append([]byte("v10"), 10)
	p.append([]byte("v11"), 11)
	p.append([]byte("v12"), 12)
	p.append([]byte("v13"), 13)

	for i, rev := range []int64{2, 3, 4, 10, 11, 12, 13} {
		index, found := p.find(rev, true)
		if index != i {
			t.Fatalf("find: expected index %d, have %d", i, index)
		}
		if !found {
			t.Fatalf("find: expected to found rev %d", rev)
		}
	}

	for _, rev := range []int64{-1, 0, 1, 5, 6, 7, 8, 9, 14, 99} {
		index, found := p.find(rev, true)
		if index != 0 {
			t.Fatalf("find: expected index 0, have %d", index)
		}
		if found {
			t.Fatalf("find: epxected to not found rev %d", rev)
		}
	}
}
