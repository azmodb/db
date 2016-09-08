package db

import (
	"reflect"
	"testing"
)

func TestPairMarshalUnmarshal(t *testing.T) {
	test := []*pair{
		&pair{
			key: []byte("some key"),
			items: []item{
				item{data: []byte("data1"), rev: 1},
				item{data: []byte("data2"), rev: 2},
				item{data: []byte("data3"), rev: 3},
			},
		},
		&pair{
			key: []byte("some key"),
			items: []item{
				item{data: []byte("data1"), rev: 1},
			},
		},
	}

	for _, p1 := range test {
		buf := make([]byte, p1.size())
		n := p1.marshal(buf)
		if n != len(buf) {
			t.Fatalf("pair encoding: expected marshal bytes %d, have %d", len(buf), n)
		}

		p2 := &pair{}
		if err := p2.unmarshal(buf); err != nil {
			t.Fatalf("pair encoding: unmarshal: %v", err)
		}

		if !reflect.DeepEqual(p1, p2) {
			t.Fatalf("pair encoding: marshal/unmarshal result differ\nexpected %#v\nhave     %#v",
				p1, p2)
		}
	}
}
