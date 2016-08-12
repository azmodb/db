package db

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

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

type Txn struct {
	txn *llrb.Txn
	rev int64
	db  *DB
}

func (t *Txn) Put(key, value []byte, tombstone bool) int64 {
	match := getPair(key)
	defer putPair(match)

	t.rev++
	if elem := t.txn.Get(match); elem != nil {
		p := elem.(*pair).copy() // TODO: optimize, if tombstone
		if tombstone {
			p.items = []item{item{data: value, rev: t.rev}}
		} else {
			p.append(value, t.rev)
		}
		t.txn.Insert(p)
	} else {
		p := newPair(key, value, t.rev)
		t.txn.Insert(p)
	}
	return t.rev
}

func (t *Txn) Delete(key []byte) int64 {
	match := getPair(key)
	defer putPair(match)

	if elem := t.txn.Get(match); elem != nil {
		t.rev++
		t.txn.Delete(match)
	}
	return t.rev
}

type tree struct {
	root *llrb.Tree
	rev  int64
}

type DB struct {
	writer sync.Mutex // exclusive writer lock
	tree   unsafe.Pointer
}

func New() *DB {
	return &DB{
		tree: unsafe.Pointer(&tree{
			root: &llrb.Tree{},
			rev:  0,
		}),
	}
}

func (db *DB) Txn() *Txn {
	db.writer.Lock()
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return &Txn{
		txn: tree.root.Txn(),
		rev: tree.rev,
		db:  db,
	}
}

func (t *Txn) Commit() {
	if t.db == nil || t.txn == nil { // already aborted or committed
		return
	}

	tree := &tree{
		root: t.txn.Commit(),
		rev:  t.rev,
	}

	atomic.StorePointer(&t.db.tree, unsafe.Pointer(tree))
	t.txn = nil
	t.rev = 0
	t.db.writer.Unlock() // release the writer lock
	t.db = nil
}

func (t *Txn) Rollback() {
	if t.db == nil || t.txn == nil { // already aborted or committed
		return
	}

	t.txn = nil
	t.db.writer.Unlock() // release the writer lock
	t.db = nil
}

func (db *DB) Get(key []byte, rev int64) ([]byte, []int64, int64) {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	match := getPair(key)
	defer putPair(match)

	if elem := tree.root.Get(match); elem != nil {
		p := elem.(*pair)
		if rev > 0 {
			item, found := p.findByRev(rev)
			if !found {
				return nil, nil, tree.rev
			}
			return item.data, p.revs(), tree.rev
		}

		item := p.last()
		return item.data, p.revs(), tree.rev
	}
	return nil, nil, tree.rev
}

type Func func(key, value []byte, revs []int64, rev int64) bool

func (db *DB) Range(from, to []byte, rev int64, fn Func) int64 {
	tree := (*tree)(atomic.LoadPointer(&db.tree))

	f := func(elem llrb.Element) bool {
		p := elem.(*pair)
		var item item
		if rev > 0 {
			var found bool
			item, found = p.findByRev(rev)
			if !found {
				return false
			}
		} else {
			item = p.last()
		}
		return fn(p.key, item.data, p.revs(), tree.rev)
	}

	fromPair, toPair := getPair(from), getPair(to)
	defer func() {
		putPair(fromPair)
		putPair(toPair)
	}()
	tree.root.Range(fromPair, toPair, f)
	return tree.rev
}

func (db *DB) Rev() int64 {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return tree.rev
}

func (db *DB) Len() int {
	tree := (*tree)(atomic.LoadPointer(&db.tree))
	return tree.root.Len()
}
