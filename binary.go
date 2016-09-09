package db

import (
	"encoding/binary"
	"errors"
	"io"
)

func readUvarint(r io.ByteReader) (uint64, int, error) {
	var v uint64
	for shift, n := uint(0), 0; ; shift += 7 {
		b, err := r.ReadByte()
		if err != nil {
			return v, n, err
		}
		n++

		if b < 0x80 {
			if n > 10 || n == 10 && b > 1 {
				return v, n, errors.New("uvarint: 64-bit overflow")
			}
			return v | uint64(b)<<shift, n, nil
		}
		v |= uint64(b&0x7f) << shift
	}
}

type uvarintReader struct {
	r   io.Reader
	buf [1]byte
}

func (ur uvarintReader) ReadByte() (byte, error) {
	if _, err := ur.r.Read(ur.buf[:]); err != nil {
		return 0, err
	}
	return ur.buf[0], nil
}

func putUvarint(buf []byte, v interface{}) int {
	switch t := v.(type) {
	case int64:
		return binary.PutUvarint(buf, uint64(t))
	case int:
		return binary.PutUvarint(buf, uint64(t))
	case uint64:
		return binary.PutUvarint(buf, t)
	}
	panic("putUvarint: unsupported type")
}

func uvarint(buf []byte) (uint64, int, error) {
	m, n := binary.Uvarint(buf)
	switch {
	case n == 0:
		return 0, n, errors.New("uvarint: buffer too small")
	case n < 0:
		return 0, n, errors.New("uvarint: 64-bit overflow")
	}
	return m, n, nil
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
