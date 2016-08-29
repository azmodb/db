//go:generate protoc --proto_path=. --go_out=. db.proto

package pb

/*
var (
	valPool = sync.Pool{New: func() interface{} { return &Value{} }}
	rowPool = sync.Pool{New: func() interface{} { return &Row{} }}
)

func getValue() *Value { return valPool.Get().(*Value) }

func putValue(m *Value) {
	m.Value = m.Value[:0]
	m.Revisions = nil
	valPool.Put(m)
}

func getRow() *Row { return rowPool.Get().(*Row) }

func putRow(m *Row) {
	m.Value = m.Value[:0]
	m.Key = m.Key[:0]
	m.Revisions = nil
	rowPool.Put(m)
}

func NewValue(value []byte, revisions []int64) *Value {
	m := getValue()

	if cap(m.Value) < len(value) {
		m.Value = make([]byte, len(value))
	}
	m.Value = m.Value[:len(value)]
	copy(m.Value, value)

	// revisions is a fresh copy, no need to copy
	m.Revisions = revisions
	return m
}

func (m *Value) Close() {
	if m == nil {
		return
	}
	putValue(m)
}

func NewRow(key, value []byte, revisions []int64) *Row {
	m := getRow()

	if cap(m.Key) < len(key) {
		m.Key = make([]byte, len(key))
	}
	m.Key = m.Key[:len(key)]
	copy(m.Key, key)

	if cap(m.Value) < len(value) {
		m.Value = make([]byte, len(value))
	}
	m.Value = m.Value[:len(value)]
	copy(m.Value, value)

	// revisions is a fresh copy, no need to copy
	m.Revisions = revisions
	return m
}

func (m *Row) Close() {
	if m == nil {
		return
	}
	putRow(m)
}
*/
