package db

import (
	"bytes"
	"sort"
	"sync"

	"github.com/azmodb/llrb"
)

var matcherPool = sync.Pool{New: func() interface{} { return &pair{} }}

// pair represents an internal immutable key/value pair. This structure
// must be kept immutable.
type pair struct {
	blocks []block
	key    []byte
	stream *stream
}

// block represents an immutable data revision. This structure must be
// kept immutable.
type block struct {
	Data interface{}
	Rev  int64
}

// newPair returns an internal immutable key/value pair. Supplied key
// and value can be reused.
func newPair(key []byte, data interface{}, rev int64) *pair {
	return &pair{
		blocks: []block{block{Data: data, Rev: rev}},
		key:    key,
		stream: &stream{},
	}
}

// matcher represents a key search query.
type matcher interface {
	llrb.Element
	release()
}

// newMatcher returns a pair to be used as search or range parameter.
func newMatcher(key []byte) matcher {
	p := matcherPool.Get().(*pair)
	p.key = key
	return p
}

// release implements the matcher interface.
func (p *pair) release() {
	p.key = nil
	matcherPool.Put(p)
}

// Compare implements the llrb.Element interface.
func (p pair) Compare(elem llrb.Element) int {
	return bytes.Compare(p.key, elem.(*pair).key)
}

// find returns the index at which rev is greater or equal, if all
// block revisions are greater find returns false.
func (p pair) find(rev int64, equal bool) (int, bool) {
	n := len(p.blocks)
	index := sort.Search(n, func(i int) bool {
		return p.blocks[i].Rev >= rev
	})
	if index == 0 && p.blocks[index].Rev > rev {
		return 0, false // revision not found
	}

	if equal {
		if index >= n || p.blocks[index].Rev != rev {
			return 0, false // revision not found
		}
	}
	if index > 0 {
		index--
	}
	return index, true
}

// inserts sets the data for a key in the pair. If ts is true insert
// sets a new tombstone pair and all previous revisions will be
// deleted.
func (p pair) insert(data interface{}, rev int64, ts bool) *pair {
	if !ts {
		n := len(p.blocks)
		blocks := make([]block, n+1)
		copy(blocks, p.blocks)
		blocks[n] = block{Data: data, Rev: rev}

		return &pair{blocks: blocks, key: p.key, stream: p.stream}
	}
	return &pair{
		blocks: []block{block{Data: data, Rev: rev}},
		key:    p.key,
		stream: p.stream,
	}
}

func (p pair) last() block { return p.blocks[len(p.blocks)-1] }

func (p pair) at(index int) block { return p.blocks[index] }

func compare(a, b []byte) int { return bytes.Compare(a, b) }
