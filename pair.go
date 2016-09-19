package db

import (
	"bytes"
	"sort"
	"sync"

	"github.com/azmodb/db/pb"
	"github.com/azmodb/llrb"
)

// Record represents a database key search query or update result. This
// structure must be kept immutable and closed after done with it. A
// record is not thread-safe.
type Record struct {
	*pb.Record
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

// newRecord allocates a new Record. newRecord does not copy the
// underlying key and values content.
func newRecord(values []*pb.Value, vtype pb.Type, cur int64) *Record {
	rec := recPool.Get().(*Record)

	rec.Values = values
	rec.Type = vtype
	rec.Current = cur

	return rec
}

func (r *Record) Close() {
	if r == nil || r.Record == nil {
		return
	}

	rec := r
	rec.Values = nil
	rec.Type = 0
	rec.Current = 0
	recPool.Put(rec)

	r = nil
}

func (r *Record) Copy() *Record {
	if r == nil || r.Record == nil {
		return nil
	}

	rec := recPool.Get().(*Record)
	n := len(r.Values)
	if cap(rec.Values) < n {
		rec.Values = make([]*pb.Value, 0, n)
	}

	rec.Current = r.Current
	rec.Type = r.Type
	for _, val := range r.Values {
		switch {
		case r.Type == pb.Unicode:
			value := unicodePool.Get().(*pb.Value)
			value.Unicode = grow(value.Unicode, val.Unicode)
			value.Rev = val.Rev
			rec.Values = append(rec.Values, value)
		case r.Type == pb.Numeric:
			value := numericPool.Get().(*pb.Value)
			value.Numeric = val.Numeric
			value.Rev = val.Rev
			rec.Values = append(rec.Values, value)
		default:
			panic("record: unsupported data type")
		}
	}
	return rec
}

// pair represents an internal immutable key/value pair. A pair value can
// be of type []byte (unicode) or int64 (numeric).
type pair struct {
	state sync.Mutex // protects key/value state
	dirty bool

	*pb.Pair
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
		p.Type = pb.Unicode
	case int64:
		val = &pb.Value{Numeric: t, Rev: rev}
		p.Type = pb.Numeric
	default:
		panic("pair: unsupported data type")
	}
	p.Values = []*pb.Value{val}

	return p
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

// copy allocates a new key/value pair. copy does not copy the
// underlying key and values content but marks the newly created
// key/value pair as dirty.
func (p *pair) copy() *pair {
	if p == nil || len(p.Values) == 0 {
		panic("pair: cannot copy uninitialized key/value pair")
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
		switch {
		case p.Type == pb.Unicode:
			pair.Values = append(pair.Values, &pb.Value{
				Unicode: val.Unicode,
				Rev:     val.Rev,
			})
		case p.Type == pb.Numeric:
			pair.Values = append(pair.Values, &pb.Value{
				Numeric: val.Numeric,
				Rev:     val.Rev,
			})
		default:
			panic("pair: unsupported data type")
		}
	}
	return pair
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
	values[n] = &pb.Value{Unicode: t, Rev: rev}
	p.Values = values
}

// tombstone sets a new data tombstone key/value pair. Previous values
// will be deleted.
func (p *pair) tombstone(value interface{}, rev int64) {
	t, ok := value.([]byte)
	if !ok {
		panic("pair: unsupported tombstone data type")
	}
	if len(p.Values) > 1 {
		p.Values = p.Values[:1:1]
	}

	val := p.Values[0]
	val.Unicode = t
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

// find finds and returns the smallest index in values at which rev is
// equal. If the equal flag is false find returns the smallest index
// at which rev is greater than.
func (p *pair) find(rev int64, equal bool) (index int, found bool) {
	index = sort.Search(len(p.Values), func(i int) bool {
		return p.Values[i].Rev >= rev
	})
	if index >= len(p.Values) { // revision not found
		return 0, found
	}

	if equal {
		if p.Values[index].Rev == rev {
			return index, true
		}
	} else {
		if p.Values[index].Rev >= rev {
			return index, true
		}
	}
	return 0, found // revision not found
}

func (p *pair) from(index int, current int64) *Record {
	values := p.Values[index:]
	return newRecord(values, p.Type, current)
}

func (p *pair) at(index int, current int64) *Record {
	values := []*pb.Value{p.Values[index]}
	return newRecord(values, p.Type, current)
}

func (p *pair) last(current int64) *Record {
	values := []*pb.Value{p.Values[len(p.Values)-1]}
	return newRecord(values, p.Type, current)
}

func (p *pair) isNum() bool { return p.Type == pb.Numeric }
