//go:generate protoc --proto_path=. --go_out=. db.proto

package pb

import (
	"encoding"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

const (
	Numeric = Type_Numeric
	Unicode = Type_Unicode
)

func MustUnmarshal(data []byte, v interface{}) {
	if err := Unmarshal(data, v); err != nil {
		panic("unmarshal failed: " + err.Error())
	}
}

func Unmarshal(data []byte, v interface{}) error {
	switch m := v.(type) {
	case encoding.BinaryUnmarshaler:
		return m.UnmarshalBinary(data)
	case proto.Message:
		return proto.Unmarshal(data, m)
	default:
		panic(fmt.Sprintf("cannot unmarshal %T", m))
	}
}

func MustMarshal(v interface{}) []byte {
	data, err := Marshal(v)
	if err != nil {
		panic("marshal failed: " + err.Error())
	}
	return data
}

func Marshal(v interface{}) ([]byte, error) {
	switch m := v.(type) {
	case encoding.BinaryMarshaler:
		return m.MarshalBinary()
	case proto.Message:
		return proto.Marshal(m)
	default:
		panic(fmt.Sprintf("cannot marshal %T", m))
	}
}
