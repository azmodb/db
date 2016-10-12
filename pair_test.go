package db

import (
	"bytes"
	"testing"
)

func TestPairAppendAndTombstone(t *testing.T) {
	test := func(p *pair, key []byte, data int64, rev int64, length int) {
		last := p.blocks[len(p.blocks)-1].Data.(int64)
		if last != data {
			t.Fatalf("pair: expected current value %d, have %d", data, last)
		}
		if bytes.Compare(p.key, key) != 0 {
			t.Fatalf("pair: expected key %q, have %q", key, p.key)
		}
		if len(p.blocks) != length {
			t.Fatalf("pair: expected %d revs, have %d", length, len(p.blocks))
		}
	}

	key := []byte("k")
	p := newPair(key, int64(2), 2)
	test(p, key, 2, 2, 1)

	p = p.insert(int64(3), 3, false)
	test(p, key, 3, 3, 2)

	p = p.insert(int64(4), 4, false)
	test(p, key, 4, 4, 3)

	p = p.insert(int64(5), 5, true)
	test(p, key, 5, 5, 1)

	p = p.insert(int64(6), 6, true)
	test(p, key, 6, 6, 1)
}

func TestPairFind(t *testing.T) {
	p := newPair([]byte("k"), 0, 10)
	p = p.insert(0, 11, false)
	p = p.insert(0, 12, false)
	p = p.insert(0, 13, false)
	p = p.insert(0, 14, false)

	test := []int64{1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 42}
	want := []struct {
		index int
		found bool
	}{
		{0, false},
		{0, false},
		{0, false},
		{0, false},
		{0, false},
		{0, false},

		{0, true},
		{0, true},
		{1, true},
		{2, true},
		{3, true},

		{4, true},
		{4, true},
		{4, true},
	}
	for i, n := range test {
		index, found := p.find(n, false)
		if want[i].found != found {
			t.Fatalf("find#%d: expected result %v, got %v", i, want[i].found, found)
		}
		if want[i].index != index {
			t.Fatalf("find#%d: expected index %d, got %d", i, want[i].index, index)
		}
	}
}

func TestPairFindEqual(t *testing.T) {
	p := newPair([]byte("k"), 0, 10)
	p = p.insert(0, 11, false)
	p = p.insert(0, 12, false)
	p = p.insert(0, 13, false)
	p = p.insert(0, 14, false)

	test := []int64{1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 42}
	want := []struct {
		index int
		found bool
	}{
		{0, false},
		{0, false},
		{0, false},
		{0, false},
		{0, false},
		{0, false},

		{0, true},
		{0, true},
		{1, true},
		{2, true},
		{3, true},

		{0, false},
		{0, false},
		{0, false},
	}
	for i, n := range test {
		index, found := p.find(n, true)
		if want[i].found != found {
			t.Fatalf("find#%d: expected result %v, got %v", i, want[i].found, found)
		}
		if want[i].index != index {
			t.Fatalf("find#%d: expected index %d, got %d", i, want[i].index, index)
		}
	}
}
