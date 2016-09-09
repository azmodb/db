package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/azmodb/llrb"
)

const (
	maxBuf        = binary.MaxVarintLen64 + binary.MaxVarintLen64
	magic  uint32 = 0xEB0BDAED
)

var table = crc32.MakeTable(crc32.Castagnoli)

type Snapshotter struct {
	tree *tree

	buf  [maxBuf]byte
	data []byte

	ur *uvarintReader // read-only ByteReader
}

func (s *Snapshotter) writeHeader(w io.Writer) (int64, error) {
	binary.LittleEndian.PutUint32(s.buf[0:4], magic)
	binary.LittleEndian.PutUint64(s.buf[4:12], uint64(s.tree.rev))
	s.buf[12] = 0x2d
	n, err := w.Write(s.buf[:13])
	return int64(n), err
}

func (s *Snapshotter) readHeader(r io.Reader) (int64, error) {
	m, err := r.Read(s.buf[:13])
	n := int64(m)
	if err != nil {
		return n, err
	}

	marker := binary.LittleEndian.Uint32(s.buf[0:4])
	if marker != magic {
		return n, errors.New("invalid database")
	}
	rev := binary.LittleEndian.Uint64(s.buf[4:12])
	if rev > math.MaxInt64 {
		return n, errors.New("invalid revision")
	}
	if s.buf[12] != 0x2d {
		return n, errors.New("malformed database")
	}

	s.tree = &tree{root: &llrb.Tree{}, rev: int64(rev)}
	return n, nil
}

func (s *Snapshotter) size() (n int) {
	s.tree.root.ForEach(func(elem llrb.Element) bool {
		p := elem.(*pair)
		size := p.size()
		n += uvarintSize(uint64(size))
		n += size
		return false
	})
	return n
}

func (s *Snapshotter) writeSize(w io.Writer) (int64, error) {
	n := putUvarint(s.buf[0:], s.size())
	n, err := w.Write(s.buf[:n])
	return int64(n), err
}

func (s *Snapshotter) readSize() (int64, int64, error) {
	v, n, err := readUvarint(s.ur)
	return int64(v), int64(n), err
}

func (s *Snapshotter) writeTo(w io.Writer) (n int64, err error) {
	s.tree.root.ForEach(func(elem llrb.Element) bool {
		p := elem.(*pair)
		size := p.size()
		s.grow(size)
		p.marshal(s.data)

		m := putUvarint(s.buf[0:], size)
		m, err = w.Write(s.buf[:m])
		n += int64(m)
		if err != nil {
			return true
		}

		m, err = w.Write(s.data)
		n += int64(m)
		if err != nil {
			return true
		}
		return false
	})
	return n, err
}

func (s *Snapshotter) readPair(r io.Reader) (*pair, int64, error) {
	size, m, err := readUvarint(s.ur)
	n := int64(m)
	if err != nil {
		return nil, n, err
	}
	s.grow(int(size))

	m, err = r.Read(s.data)
	n += int64(m)
	if err != nil {
		return nil, n, err
	}

	p := &pair{}
	if err = p.unmarshal(s.data); err != nil {
		return nil, n, err
	}
	return p, n, nil
}

func (db *DB) Snapshot() *Snapshotter {
	return &Snapshotter{
		data: make([]byte, 32*1024),
		tree: db.load(),
	}
}

func (s *Snapshotter) WriteTo(w io.Writer) (n int64, err error) {
	if n, err = s.writeHeader(w); err != nil {
		return n, err
	}

	m, err := s.writeSize(w)
	n += m
	if err != nil {
		return n, err
	}

	m, err = s.writeTo(w)
	n += m
	return n, err
}

func ReadFrom(r io.Reader) (db *DB, n int64, err error) {
	// tree: is initialized by s.readHeader(reader)
	s := &Snapshotter{
		ur:   &uvarintReader{r: r},
		data: make([]byte, 32*1024),
	}

	if n, err = s.readHeader(r); err != nil {
		return nil, n, err
	}

	size, m, err := s.readSize()
	n += m
	if err != nil {
		return nil, n, err
	}
	fmt.Println("read size", size)

	txn := s.tree.root.Txn()
	var off int64
	for off < size {
		p, done, err := s.readPair(r)
		off += done
		if err != nil {
			return nil, n, err
		}
		txn.Insert(p)
	}
	s.tree.root = txn.Commit()
	n += off

	return newDB(s.tree), n, err
}

func (s *Snapshotter) grow(size int) {
	if cap(s.data) < size {
		s.data = make([]byte, size)
	}
	s.data = s.data[:size]
}

func (s *Snapshotter) Rev() int64 { return s.tree.rev }

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
