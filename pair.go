package db

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/azmodb/llrb"
)

// item represents an internal immutable value. value interface can be of
// type []byte or int64.
type item struct {
	value interface{}
	rev   int64
}

// pair represents an internal immutable key/value pair.
type pair struct {
	key   []byte
	items []item
}

var matcherPool = sync.Pool{New: func() interface{} { return &pair{} }}

// newPair creates a new internal immutable key/value pair. newPair
// creates a copy of the key. If value is of type []byte newPair
// creates a copy of the value byte slice.
//
// If value is not of type []byte or int64 newPair will panic.
func newPair(key []byte, value interface{}, rev int64) *pair {
	p := &pair{key: bcopy(key)}
	switch t := value.(type) {
	case []byte:
		p.items = []item{item{value: bcopy(t), rev: rev}}
	case int64:
		p.items = []item{item{value: t, rev: rev}}
	default:
		panic("new pair: invalid value type")
	}
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
	p.key = key
	return p
}

// Close implementes the matcher interface.
func (p *pair) Close() {
	p.key = nil
	matcherPool.Put(p)
}

// append appends a single item. Updates should rarely happen more than
// once per transaction in practice.
func (p *pair) append(value []byte, rev int64) {
	n := len(p.items)
	items := make([]item, n+1)
	copy(items, p.items)

	items[n] = item{value: bcopy(value), rev: rev}
	p.items = items
}

// tombstone creates a new data tombstone key/value pair.
func (p *pair) tombstone(value []byte, rev int64) {
	if len(p.items) == 0 {
		p.items = []item{item{value: bcopy(value), rev: rev}}
	}
	if len(p.items) >= 1 {
		p.items = p.items[:1:1]
	}

	v := p.items[0]
	v.value = bcopy(value)
	v.rev = rev
	p.items[0] = v
}

// increment increments the underlying numeric value.
func (p *pair) increment(value int64, rev int64) {
	v := p.items[0]

	v.value = v.value.(int64) + value
	v.rev = rev
	p.items[0] = v
}

// last returns the most recent value and revision.
func (p *pair) last() (interface{}, int64) {
	v := p.items[len(p.items)-1]
	return v.value, v.rev
}

// find returns the value and revision at revision. If strict is true
// the given rev must match a revision of the pair.
// If strict is false and the pair contains a revision greater of equal
// than rev find returns the most current data and revision.
//
// find returns false if the revision does not exists.
func (p *pair) find(rev int64, strict bool) (interface{}, int64, bool) {
	i := sort.Search(len(p.items), func(i int) bool {
		return p.items[i].rev >= rev
	})

	if i == 0 && len(p.items) > 0 {
		if item, found := p.isValid(0, rev, strict); found {
			return item.value, item.rev, true
		}
		return nil, 0, false
	}

	if i <= 0 || i >= len(p.items) { // not found
		return nil, 0, false
	}

	if item, found := p.isValid(i, rev, strict); found {
		return item.value, item.rev, true
	}
	return nil, 0, false
}

var nilItem = item{}

func (p *pair) isValid(index int, rev int64, strict bool) (item, bool) {
	if !strict {
		return p.items[len(p.items)-1], true
	} else {
		v := p.items[index]
		if v.rev == rev {
			return v, true
		}
	}
	return nilItem, false
}

// Compare implements llrb.Element.
func (p *pair) Compare(elem llrb.Element) int {
	return bytes.Compare(p.key, elem.(*pair).key)
}

// copy creates a new pair and items slice. copy does not copy the
// underlying key and values.
func (p *pair) copy() *pair {
	if p == nil {
		panic("cannot copy <nil> pair")
	}

	np := &pair{
		key:   p.key,
		items: make([]item, 0, len(p.items)),
	}
	for _, i := range p.items {
		np.items = append(np.items, item{
			value: i.value,
			rev:   i.rev,
		})
	}
	return np
}

// revs returns all revisions.
func (p *pair) revs() []int64 {
	revs := make([]int64, 0, len(p.items))
	for _, item := range p.items {
		revs = append(revs, item.rev)
	}
	return revs
}

const (
	numericType byte = 1
	valueType   byte = 2
)

// bytes slice encoding (bytes) = uvarint64 length + content
//
// key (bytes) |

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (p *pair) MarshalBinary() ([]byte, error) {
	buf := make([]byte, p.size())
	p.marshal(buf)
	return buf, nil
}

func (p *pair) marshal(buf []byte) (n int) {
	n = putUvarint(buf[0:], len(p.key))
	n += copy(buf[n:], p.key)
	n += putUvarint(buf[n:], len(p.items))

	for _, item := range p.items {
		switch t := item.value.(type) {
		case []byte:
			buf[n] = valueType
			n++
			n += putUvarint(buf[n:], len(t))
			n += copy(buf[n:], t)
		case int64:
			buf[n] = numericType
			n++
			n += putUvarint(buf[n:], t)
		default:
			panic("marshal: invalid item type")
		}
		n += putUvarint(buf[n:], item.rev)
	}
	return n
}

func (p *pair) size() (n int) {
	n += uvarintSize(uint64(len(p.key))) + len(p.key)
	n += uvarintSize(uint64(len(p.items)))

	for _, item := range p.items {
		switch t := item.value.(type) {
		case []byte:
			n += 1 + uvarintSize(uint64(len(t))) + len(t)
		case int64:
			n += 1 + uvarintSize(uint64(t))
		default:
			panic("size: invalid item type")
		}
		n += uvarintSize(uint64(item.rev))
	}
	return n
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (p *pair) UnmarshalBinary(buf []byte) error {
	return p.unmarshal(buf)
}

func (p *pair) unmarshal(buf []byte) error {
	np := &pair{}
	v, n, err := uvarint(buf[0:]) // unmarshal pair key bytes
	m := n + int(v)
	if err != nil {
		return err
	}
	np.key = bcopy(buf[n:m])

	v, n, err = uvarint(buf[m:]) // unmarshal items count
	m += n
	if err != nil {
		return err
	}
	np.items = make([]item, 0, v) // TODO: overflow

	for m < len(buf) {
		typ := buf[m]
		m++

		v, n, err := uvarint(buf[m:])
		m += n
		if err != nil {
			return err
		}

		var value interface{}
		switch {
		case typ == valueType:
			value = bcopy(buf[m : m+int(v)])
			m += int(v)
		case typ == numericType:
			if v > math.MaxInt64 {
				return errors.New("unmarshal: malformed item")
			}
			value = int64(v)
		default:
			return errors.New("unmarshal: invalid item type")
		}

		rev, n, err := uvarint(buf[m:])
		if err != nil {
			return err
		}
		m += n

		np.items = append(np.items, item{value, int64(rev)})
	}
	if len(np.items) != cap(np.items) {
		return errors.New("unmarshal: malformed pair")
	}

	*p = *np
	return nil
}

func bcopy(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
