package db

import "testing"

func TestItemFind(t *testing.T) {
	p := &pair{
		items: []item{
			{[]byte("v1"), 1},
			{[]byte("v2"), 3},
			{[]byte("v3"), 5},
			{[]byte("v4"), 7},
		},
	}

	item, index, found := p.find(1)
	if string(item.data) != "v1" {
		t.Fatalf("find item: expected value %q, got %q", "v1", item.data)
	}
	if index != 0 {
		t.Fatalf("find item: expected item index %d, got %d", 0, index)
	}
	if found != true {
		t.Fatalf("find item: item not found")
	}

	item, index, found = p.find(3)
	if string(item.data) != "v2" {
		t.Fatalf("find item: expected value %q, got %q", "v2", item.data)
	}
	if index != 1 {
		t.Fatalf("find item: expected item index %d, got %d", 1, index)
	}
	if found != true {
		t.Fatalf("find item: item not found")
	}

	item, index, found = p.find(7)
	if string(item.data) != "v4" {
		t.Fatalf("find item: expected value %q, got %q", "v4", item.data)
	}
	if index != 3 {
		t.Fatalf("find item: expected item index %d, got %d", 3, index)
	}
	if found != true {
		t.Fatalf("find item: item not found")
	}

	item, index, found = p.find(8)
	if string(item.data) != "" {
		t.Fatalf("find item: expected empty value, got %q", item.data)
	}
	if index != -1 {
		t.Fatalf("find item: expected zero index, got %d", index)
	}
	if found != false {
		t.Fatalf("find item: item found")
	}

	item, index, found = p.find(0)
	if string(item.data) != "" {
		t.Fatalf("find item: expected empty value, got %q", item.data)
	}
	if index != -1 {
		t.Fatalf("find item: expected zero index, got %d", index)
	}
	if found != false {
		t.Fatalf("find item: item found")
	}

	item, index, found = p.find(-1)
	if string(item.data) != "" {
		t.Fatalf("find item: expected empty value, got %q", item.data)
	}
	if index != -1 {
		t.Fatalf("find item: expected zero index, got %d", index)
	}
	if found != false {
		t.Fatalf("find item: item found")
	}
}

/*
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

		value, _, rev := tree.Get([]byte("k2"), 0)
		fmt.Println(string(value), rev)

		value, _, rev = tree.Get([]byte("k2"), 4)
		fmt.Println(string(value), rev)

		value, _, rev = tree.Get([]byte("k2"), 3)
		fmt.Println(string(value), rev)

		value, _, rev = tree.Get([]byte("k2"), 2)
		fmt.Println(string(value), rev)

		txn = tree.Txn()

		txn.Delete([]byte("k2"), 4)

		txn.Commit()

		value, _, rev = tree.Get([]byte("k2"), 4)
		fmt.Println(string(value), rev)
		/*
			fmt.Println("---")
			tree.Range([]byte("k1"), []byte("k6"), func(v []byte, rev int64) bool {
				fmt.Println(string(v), rev)
				return false
			})
*/
