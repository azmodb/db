//go:generate protoc --proto_path=. --go_out=. db.proto

package pb

import "github.com/gogo/protobuf/proto"

const (
	Numeric = Type_Numeric
	Unicode = Type_Unicode
)

func MustMarshal(m proto.Message) []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		panic("protobuf marshal failed: " + err.Error())
	}
	return data
}
