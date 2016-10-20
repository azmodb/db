package db

import (
	"encoding/gob"
	"io"
)

func decode(buf *buffer, blocks *[]block) error {
	return gob.NewDecoder(buf).Decode(blocks)
}

func encode(buf *buffer, blocks []block) error {
	return gob.NewEncoder(buf).Encode(blocks)
}

type buffer []byte

func newBuffer(data []byte) *buffer {
	if data == nil {
		return &buffer{}
	}
	b := &buffer{}
	*b = data
	return b
}

func (b *buffer) WriteString(s string) (int, error) {
	*b = append(*b, s...)
	return len(s), nil
}

func (b *buffer) Write(p []byte) (int, error) {
	*b = append(*b, p...)
	return len(p), nil
}

func (b *buffer) WriteByte(p byte) error {
	*b = append(*b, p)
	return nil
}

func (b buffer) Len() int      { return len(b) }
func (b buffer) Bytes() []byte { return b }
func (b *buffer) Reset()       { *b = (*b)[:0] }

func (b *buffer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(*b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, *b)
	*b = (*b)[n:]
	return n, nil
}

func (b *buffer) ReadByte() (byte, error) {
	if len(*b) == 0 {
		return 0, io.EOF
	}

	p := (*b)[0]
	*b = (*b)[1:]
	return p, nil
}
