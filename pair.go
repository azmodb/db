package db

import (
	"bytes"
	"sort"
	"sync"

	"github.com/azmodb/llrb"
)

var pairPool = sync.Pool{New: func() interface{} { return &pair{} }}

type item struct {
	data []byte
	rev  int64
}

type pair struct {
	key   []byte
	items []item
}

func newPair(key []byte, value []byte, rev int64) *pair {
	return &pair{
		key:   key,
		items: []item{item{data: value, rev: rev}},
	}
}

func getPair(key []byte) *pair {
	p := pairPool.Get().(*pair)
	p.key = key
	return p
}

func putPair(p *pair) {
	p.key = nil
	pairPool.Put(p)
}

// Compare implements llrb.Element.
func (p *pair) Compare(elem llrb.Element) int {
	return bytes.Compare(p.key, elem.(*pair).key)
}

func (p *pair) copy() *pair {
	np := &pair{
		key:   p.key,
		items: make([]item, 0, len(p.items)),
	}
	for _, i := range p.items {
		np.items = append(np.items, item{
			data: i.data,
			rev:  i.rev,
		})
	}
	return np
}

func (p *pair) append(value []byte, rev int64) {
	n := len(p.items)
	items := make([]item, n+1)
	copy(items, p.items)
	items[n] = item{data: value, rev: rev}
	p.items = items
}

func (p *pair) last() item {
	return p.items[len(p.items)-1]
}

var nilItem = item{}

func (p *pair) findByRev(rev int64) (item, bool) {
	i := sort.Search(len(p.items), func(i int) bool {
		return p.items[i].rev >= rev
	})

	if i >= len(p.items) { // p.items[i] < rev
		return p.items[i-1], true
	}
	if p.items[i].rev == rev {
		return p.items[i], true
	}
	if p.items[i].rev > rev {
		if i == 0 {
			return nilItem, false
		}
		return p.items[i-1], true
	}
	return nilItem, false
}

func (p *pair) revs() []int64 {
	revs := make([]int64, 0, len(p.items))
	for _, item := range p.items {
		revs = append(revs, item.rev)
	}
	return revs
}