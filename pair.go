package db

import (
	"bytes"
	"sort"
	"sync"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

// pair represents an internal immutable key/value pair. A pair value can
// be of type []byte (unicode) or int64 (numeric).
type pair struct {
	sync.Mutex
	dirty bool

	*pb.Pair
}

const (
	// numeric represents the numeric value data type
	numeric int32 = iota + 1

	// unicode represents the unicode value data type
	unicode
)

type Record struct {
	*pb.Record
}

// newPair returns an internal immutable key/value pair. A pair value
// can be of type []byte (unicode) or int64 (numeric). Supplies key
// and value can be reused.
func newPair(key []byte, value interface{}, rev int64) *pair {
	p := &pair{Pair: &pb.Pair{Key: bcopy(key)}, dirty: true}

	var val *pb.Value
	switch t := value.(type) {
	case []byte:
		val = &pb.Value{Unicode: bcopy(t), Rev: rev}
		p.Type = unicode
	case int64:
		val = &pb.Value{Numeric: t, Rev: rev}
		p.Type = numeric
	default:
		panic("pair: unsupported data type")
	}
	p.Values = []*pb.Value{val}

	return p
}

var (
	recPool = sync.Pool{
		New: func() interface{} { return &Record{Record: &pb.Record{}} },
	}
	unicodePool = sync.Pool{
		New: func() interface{} { return &pb.Value{} },
	}
	numericPool = sync.Pool{
		New: func() interface{} { return &pb.Value{} },
	}
	matcherPool = sync.Pool{ // key search query matcher pool
		New: func() interface{} { return &pair{Pair: &pb.Pair{}} },
	}
)

// cloneRecord allocates a new value record. cloneRecord allocates a new
// key slice. If the pair value is of type unicode, cloneRecord
// allocates a new value slice.
func cloneRecord(values []*pb.Value, vtype int32, cur int64) *Record {
	rec := recPool.Get().(*Record)
	n := len(values)
	if cap(rec.Values) < n {
		rec.Values = make([]*pb.Value, 0, n)
	}

	rec.Current = cur
	rec.Type = vtype

	for _, val := range values {
		if vtype == unicode {
			value := unicodePool.Get().(*pb.Value)
			n := len(val.Unicode)
			if cap(value.Unicode) < n {
				value.Unicode = make([]byte, n)
			}
			value.Unicode = value.Unicode[:n]
			copy(value.Unicode, val.Unicode)

			value.Rev = val.Rev
			rec.Values = append(rec.Values, value)
		} else {
			value := numericPool.Get().(*pb.Value)
			value.Numeric = val.Numeric
			value.Rev = val.Rev
			rec.Values = append(rec.Values, value)
		}
	}
	rec.Values = rec.Values[:n]
	return rec
}

func (r *Record) Close() {
	if r == nil || r.Record == nil {
		return
	}

	for _, val := range r.Values {
		if r.Type == unicode {
			val.Unicode = val.Unicode[:0] // TODO: remove too big slices
			val.Rev = 0
			unicodePool.Put(val)
		} else {
			val.Numeric = 0
			val.Rev = 0
			numericPool.Put(val)
		}
	}

	rec := r
	rec.Values = rec.Values[:0]
	rec.Type = 0
	rec.Current = 0
	recPool.Put(rec)

	r = nil
}

// clone allocates a new key/value pair. clone does not copy the
// underlying key and values content.
func (p *pair) clone() *pair {
	if p == nil || len(p.Values) == 0 {
		panic("pair: cannot clone uninitialized key/value pair")
	}

	pair := &pair{
		Pair: &pb.Pair{
			Values: make([]*pb.Value, 0, len(p.Values)),
			Key:    p.Key,
			Type:   p.Type,
		},
		dirty: true,
	}
	for _, val := range p.Values {
		if p.Type == unicode {
			pair.Values = append(pair.Values, &pb.Value{
				Unicode: val.Unicode,
				Rev:     val.Rev,
			})
		} else {
			pair.Values = append(pair.Values, &pb.Value{
				Numeric: val.Numeric,
				Rev:     val.Rev,
			})
		}
	}
	return pair
}

// matcher represents a key search query.
type matcher interface {
	llrb.Element
	Close()
}

// newMatcher returns a new matcher to be used as get and range query
// parameter.
func newMatcher(key []byte) matcher {
	p := matcherPool.Get().(*pair)
	p.Key = key
	return p
}

// Compare implements llrb.Element and matcher.
func (p *pair) Compare(elem llrb.Element) int {
	return bytes.Compare(p.Key, elem.(*pair).Key)
}

// Close implementes the matcher interface.
func (p *pair) Close() {
	p.Key = nil
	matcherPool.Put(p)
}

// append appends a single unicode value. Updates should rarely happen
// more than once per transaction in practice.
func (p *pair) append(value interface{}, rev int64) {
	t, ok := value.([]byte)
	if !ok {
		panic("pair: unsupported append data type")
	}

	n := len(p.Values)
	values := make([]*pb.Value, n+1)
	copy(values, p.Values)
	values[n] = &pb.Value{Unicode: bcopy(t), Rev: rev}
	p.Values = values
}

// tombstone sets a new data tombstone key/value pair. Previous values
// will be deleted.
func (p *pair) tombstone(value interface{}, rev int64) {
	t, ok := value.([]byte)
	if !ok {
		panic("pair: unsupported tombstone data type")
	}
	if len(p.Values) >= 1 {
		p.Values = p.Values[:1:1]
	}

	val := p.Values[0]
	val.Unicode = bcopy(t)
	val.Rev = rev
	p.Values[0] = val
}

// increment increments the underlying numeric value.
func (p *pair) increment(value interface{}, rev int64) {
	t, ok := value.(int64)
	if !ok {
		panic("pair: unsupported increment data type")
	}

	val := p.Values[0]
	val.Numeric += t
	val.Rev = rev
	p.Values[0] = val
}

func (p *pair) find(rev int64) (index int, found bool) {
	index = sort.Search(len(p.Values), func(i int) bool {
		return p.Values[i].Rev >= rev
	})
	if index >= len(p.Values) { // revision not found
		return 0, found
	}

	if p.Values[index].Rev == rev {
		return index, true
	}
	return 0, found // revision not found
}

func (p *pair) from(index int, cur int64) *Record {
	values := p.Values[index:]
	return cloneRecord(values, p.Type, cur)
}

func (p *pair) at(index int, cur int64) *Record {
	values := []*pb.Value{p.Values[index]}
	return cloneRecord(values, p.Type, cur)
}

func (p *pair) last(cur int64) *Record {
	values := []*pb.Value{p.Values[len(p.Values)-1]}
	return cloneRecord(values, p.Type, cur)
}

func (p *pair) isNum() bool { return p.Type == numeric }

func bcopy(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
