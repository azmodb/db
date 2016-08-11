package db

import (
	"fmt"
	"testing"
)

/*
func TestItemFind(t *testing.T) {
	p := &pair{
		items: []item{
			{[]byte("v1"), 1},
			{[]byte("v2"), 3},
			{[]byte("v3"), 5},
			{[]byte("v4"), 7},
		},
	}

	item, found := p.find(3)
	fmt.Printf("%+v %v\n", item, found)

	item, found = p.find(5)
	fmt.Printf("%+v %v\n", item, found)

	item, found = p.find(1)
	fmt.Printf("%+v %v\n", item, found)

	item, found = p.find(0)
	fmt.Printf("%+v %v\n", item, found)

	item, found = p.find(8)
	fmt.Printf("%+v %v\n", item, found)
}
*/

func TestBasic(t *testing.T) {
	tree := New()
	txn := tree.Txn()

	txn.Put([]byte("k1"), []byte("v1.1"))

	txn.Put([]byte("k2"), []byte("v2.1"))
	txn.Put([]byte("k2"), []byte("v2.2"))
	txn.Put([]byte("k2"), []byte("v2.3"))
	txn.Put([]byte("k2"), []byte("v2.4"))
	txn.Put([]byte("k2"), []byte("v2.5"))

	txn.Put([]byte("k3"), []byte("v3.1"))
	txn.Put([]byte("k4"), []byte("v4.1"))
	txn.Put([]byte("k5"), []byte("v5.1"))

	txn.Commit()

	fmt.Println(tree.Len(), tree.Rev())

	value, rev := tree.Get([]byte("k2"), 0)
	fmt.Println(string(value), rev)

	value, rev = tree.Get([]byte("k2"), 4)
	fmt.Println(string(value), rev)

	value, rev = tree.Get([]byte("k2"), 3)
	fmt.Println(string(value), rev)

	value, rev = tree.Get([]byte("k2"), 2)
	fmt.Println(string(value), rev)

	txn = tree.Txn()

	txn.Delete([]byte("k2"), 4)

	txn.Commit()

	value, rev = tree.Get([]byte("k2"), 4)
	fmt.Println(string(value), rev)
	/*
		fmt.Println("---")
		tree.Range([]byte("k1"), []byte("k6"), func(v []byte, rev int64) bool {
			fmt.Println(string(v), rev)
			return false
		})
	*/
}
