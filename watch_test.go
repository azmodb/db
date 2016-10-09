package db

import "testing"

func TestBasicWatcher(t *testing.T) {
	count := 100
	key := []byte("k")
	db := New()
	tx := db.Txn()
	tx.Put(key, 0, false)
	tx.Commit()

	w, _, err := db.Watch(key)
	if err != nil {
		t.Fatalf("create watcher: %v", err)
	}
	defer w.Close()

	done := make(chan struct{})
	go func() {
		n := 0
		for ev := range w.Recv() {
			if ev.Err() != nil {
				if n != count || ev.Err() != watcherCanceled {
					t.Fatalf("watcher: expected count %d and error %v, have %d %v",
						count, watcherCanceled, n, ev.Err())
				}
				break
			}
			n++
			if ev.Current != int64(n+1) {
				t.Fatalf("watcher: expected current revision %d, have %d", int64(n+1),
					ev.Current)
			}
			if ev.Created != int64(n+1) {
				t.Fatalf("watcher: expected created revision %d, have %d", int64(n+1),
					ev.Created)
			}
			if ev.Data.(int) != n {
				t.Fatalf("watcher: expected value %d, have %d", n, ev.Data.(int))
			}
		}
		close(done)
	}()

	tx = db.Txn()
	for i := 1; i <= count; i++ {
		tx.Put(key, i, false)
	}
	tx.Commit()

	w.Close()
	<-done
}

func TestWatcherCloseByUser(t *testing.T) {
	s := &stream{}
	w := s.Register()

	w.Close()
	if w.id != -1 {
		t.Fatalf("watcher: expected zero id, have %d", w.id)
	}

	w.Close()
	if w.id != -1 {
		t.Fatalf("watcher: expected zero id, have %d", w.id)
	}
}

func TestWatcherClosed(t *testing.T) {
	s := &stream{}
	w := s.Register()

	s.Close()
	if s.running {
		t.Fatalf("watcher: stream not closed")
	}

	if w.id != -1 {
		t.Fatalf("watcher: expected zero id, have %d", w.id)
	}
}
