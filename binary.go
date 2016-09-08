package db

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	maxBuf        = binary.MaxVarintLen64 + binary.MaxVarintLen64
	magic  uint32 = 0xEB0BDAED
)

var table = crc32.MakeTable(crc32.Castagnoli)

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
		return 0, n, Error("uvarint: 64 bit overflow")
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
