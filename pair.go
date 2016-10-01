package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

var (
	pbrecPool   = sync.Pool{New: func() interface{} { return &pb.Record{} }}
	pbblockPool = sync.Pool{New: func() interface{} { return &pb.Block{} }}

	matcherPool = sync.Pool{New: func() interface{} { return &pair{} }}
)

// maxPoolDataSize defines the maximal unicode type pool size.
const maxPoolDataSize = 2 * 8192 // TODO: find capacity

const maxInt = uint64(^uint(0) >> 1)

const (
	numeric byte = 0x01
	unicode byte = 0x02
)

// pair represents an internal immutable key/value pair. This structure
// must be kept immutable.
type pair struct {
	blocks []block
	key    string
	typ    byte
}

// block represents an immutable value revision. Data can be of type
// int64 or []byte. This structure must be kept immutable.
type block struct {
	data interface{}
	rev  int64
}

func (b block) bytes() []byte { return b.data.([]byte) }
func (b block) num() int64    { return b.data.(int64) }

func (b block) isNum() bool {
	_, ok := b.data.(int64)
	return ok
}

func (b block) size() (n int) {
	if b.data == nil {
		panic("block: cannot serialize <nil> block data type")
	}

	switch t := b.data.(type) {
	case []byte:
		n += uvarintSize(uint64(len(t))) + len(t)
	case int64:
		n += uvarintSize(uint64(t))
	default:
		panic(fmt.Sprintf("block: unsupported data type (%T)", t))
	}
	n += uvarintSize(uint64(b.rev))
	return n
}

func (b block) marshal(data []byte) (n int) {
	if b.data == nil {
		panic("block: cannot serialize <nil> block data type")
	}

	switch t := b.data.(type) {
	case []byte:
		n += binary.PutUvarint(data[n:], uint64(len(t)))
		n += copy(data[n:], t)
	case int64:
		n += binary.PutUvarint(data[n:], uint64(t))
	default:
		panic(fmt.Sprintf("block: unsupported data type (%T)", t))
	}
	n += binary.PutUvarint(data[n:], uint64(b.rev))
	return n
}

func (b *block) unmarshal(typ byte, data []byte) (n int, err error) {
	var v uint64
	var m int

	switch typ {
	case unicode:
		v, m, err = uvarint(data[n:])
		n += m
		if err != nil {
			return n, err
		}
		if v > maxInt {
			return n, errors.New("block: integer overflow")
		}
		value := make([]byte, int(v))
		n += copy(value, data[n:n+int(v)])
		b.data = value
	case numeric:
		v, m, err = uvarint(data[n:])
		n += m
		if err != nil {
			return n, err
		}
		if v > math.MaxInt64 {
			return n, errors.New("block: integer overflow")
		}
		b.data = int64(v)
	default:
		panic(fmt.Sprintf("block: unsupported data type (#%d)", typ))
	}

	v, m, err = uvarint(data[n:])
	n += m
	if err != nil {
		return n, err
	}
	if v > math.MaxInt64 {
		return n, errors.New("block: integer overflow")
	}
	b.rev = int64(v)

	return n, err
}

// newPair returns an internal immutable key/value pair. A pair value
// can be of type []byte (unicode) or int64 (numeric). Supplied key
// and value can be reused.
func newPair(key string, value interface{}, rev int64) *pair {
	p := &pair{key: key}
	b := block{rev: rev}

	switch t := value.(type) {
	case []byte:
		b.data = clone(nil, t)
		p.typ = unicode
	case int64:
		b.data = t
		p.typ = numeric
	default:
		panic(fmt.Sprintf("pair: unsupported data type (%T)", t))
	}
	p.blocks = []block{b}

	return p
}

// matcher represents a key search query.
type matcher interface {
	llrb.Element
	release()
}

// newMatcher returns a new matcher to be used as get and range query
// parameter.
func newMatcher(key string) matcher {
	p := matcherPool.Get().(*pair)
	p.key = key
	return p
}

// Compare implements the llrb.Element and matcher.
func (p *pair) Compare(elem llrb.Element) int {
	key := elem.(*pair).key
	if p.key == key {
		return 0
	}
	if p.key < key {
		return -1
	}
	return 1
}

// release implementes the matcher interface.
func (p *pair) release() {
	p.key = ""
	matcherPool.Put(p)
}

// find returns the smallest index in blocks at which rev is equal or
// greater than if the equal parameter is false.
func (p *pair) find(rev int64, equal bool) (index int, found bool) {
	index = sort.Search(len(p.blocks), func(i int) bool {
		return p.blocks[i].rev >= rev
	})
	if index >= len(p.blocks) { // revision not found
		return 0, found
	}

	if equal {
		if p.blocks[index].rev == rev {
			return index, true
		}
	} else {
		if p.blocks[index].rev >= rev {
			return index, true
		}
	}
	return 0, found // revision not found
}

// tombstone sets a new data tombstone block. Previous blocks will be
// deleted.
func (p *pair) tombstone(value interface{}, rev int64) *pair {
	t, ok := value.([]byte)
	if !ok {
		panic(fmt.Sprintf("pair: unsupported data type (%T)", t))
	}
	if p.isNum() {
		panic("tombstone: data type mismatch")
	}

	pair := &pair{typ: p.typ, key: p.key}
	b := block{
		data: clone(nil, t),
		rev:  rev,
	}
	pair.blocks = []block{b}
	return pair
}

// insert appends a single block. Updates should rarely happen more
// than once per transaction in practice.
func (p *pair) insert(value interface{}, rev int64) *pair {
	t, ok := value.([]byte)
	if !ok {
		panic(fmt.Sprintf("pair: unsupported data type (%T)", t))
	}
	if p.isNum() {
		panic("insert: data type mismatch")
	}

	n := len(p.blocks)
	pair := &pair{
		blocks: make([]block, n+1),
		typ:    p.typ,
		key:    p.key,
	}
	copy(pair.blocks, p.blocks)
	pair.blocks[n] = block{
		data: clone(nil, t),
		rev:  rev,
	}
	return pair
}

// increment increments the underlying numeric value.
func (p *pair) increment(value interface{}, rev int64) *pair {
	t, ok := value.(int64)
	if !ok {
		panic(fmt.Sprintf("pair: unsupported data type (%T)", t))
	}
	if !p.isNum() {
		panic("increment: data type mismatch")
	}

	pair := &pair{typ: p.typ, key: p.key}
	b := block{
		data: p.blocks[0].num() + t,
		rev:  rev,
	}
	pair.blocks = []block{b}
	return pair
}

func (p *pair) isNum() bool { return p.typ == numeric }

func newProtobufRecord(typ byte, blocks []block) *pb.Record {
	n := len(blocks)
	rec := pbrecPool.Get().(*pb.Record)

	if cap(rec.Blocks) < n {
		rec.Blocks = make([]*pb.Block, n)
	}
	rec.Blocks = rec.Blocks[:n]

	if typ == numeric {
		rec.Type = pb.Record_Numeric
	} else {
		rec.Type = pb.Record_Unicode
	}

	for i, block := range blocks {
		b := pbblockPool.Get().(*pb.Block)
		b.Revision = block.rev
		switch t := block.data.(type) {
		case []byte:
			b.Unicode = clone(b.Unicode, t)
		case int64:
			b.Numeric = t
		}
		rec.Blocks[i] = b
	}
	return rec
}

func closeProtobufRecord(rec *pb.Record) {
	if rec == nil || len(rec.Blocks) == 0 {
		return
	}

	rec.Type = pb.Record_Invalid
	for _, block := range rec.Blocks {
		b := block
		switch {
		case cap(b.Unicode) > maxPoolDataSize:
			b.Unicode = b.Unicode[0:0:maxPoolDataSize]
		case b.Unicode != nil:
			b.Unicode = b.Unicode[:0]
		}
		b.Numeric = 0
		b.Revision = 0
		pbblockPool.Put(b)
		block = nil
	}
	rec.Blocks = rec.Blocks[:0]
	pbrecPool.Put(rec)
}

func (p *pair) from(index int) *pb.Record {
	return newProtobufRecord(p.typ, p.blocks[index:])
}

func (p *pair) at(index int) *pb.Record {
	return newProtobufRecord(p.typ, p.blocks[index:index+1])
}

func (p *pair) last() *pb.Record {
	index := len(p.blocks)
	return newProtobufRecord(p.typ, p.blocks[index-1:index])
}

func clone(dst, src []byte) []byte {
	n := len(src)
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	copy(dst, src)
	return dst
}

func uvarintSize(v uint64) (n int) {
	for {
		n++
		v >>= 7
		if v == 0 {
			break
		}
	}
	return n
}

func uvarint(data []byte) (uint64, int, error) {
	v, n := binary.Uvarint(data)
	if n < 0 {
		return 0, n, errors.New("value larger than 64 bits")
	}
	if n == 0 {
		return 0, n, errors.New("buffer too small")
	}
	return v, n, nil
}
