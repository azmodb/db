package db

/*
func TestDBGet(t *testing.T) {
	db := newDB(nil)
	b := db.Next()
	b.Insert([]byte("k1"), []byte("v1.1"), false)
	b.Insert([]byte("k2"), []byte("v2.1"), false)
	b.Insert([]byte("k3"), []byte("v3.1"), false)
	b.Insert([]byte("k4"), []byte("v4.1"), false)
	b.Insert([]byte("k1"), []byte("v1.2"), false)
	b.Insert([]byte("k1"), []byte("v1.3"), false)
	b.Commit()

	rec, _ := db.Get([]byte("k1"), 0, true)
	fmt.Println(rec)
	rec, _ = db.Get([]byte("k1"), 0, false)
	fmt.Println(rec)

	rec, _ = db.Get([]byte("k1"), 4, true)
	fmt.Println(rec)
	rec, _ = db.Get([]byte("k1"), 4, false)
	fmt.Println(rec)

	db.Range([]byte("k1"), []byte("k5"), 0, false, func(k []byte, r *Record) bool {
		fmt.Println(string(k), r)
		return false
	})
}
*/
