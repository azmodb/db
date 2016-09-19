//go:generate protoc --proto_path=. --go_out=. db.proto

package pb

const (
	Numeric = Type_Numeric
	Unicode = Type_Unicode
)
