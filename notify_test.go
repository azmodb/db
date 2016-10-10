package db

import "testing"

func TestBasicNotifier(t *testing.T) {
	key, count := []byte("k"), 100
	db := New()
	tx := db.Txn()
	tx.Put(key, 0, false)
	tx.Commit()

	n, _, err := db.Watch(key)
	if err != nil {
		t.Fatalf("create notifier: %v", err)
	}
	defer n.Cancel()

	done := make(chan struct{})
	go func() {
		i := 0
		for ev := range n.Recv() {
			if ev.Err() != nil {
				if i != count || ev.Err() != notifierCanceled {
					t.Fatalf("notifier: expected count %d and error %v, have %d %v",
						count, notifierCanceled, i, ev.Err())
				}
				break
			}
			i++
			if ev.Current != int64(i+1) {
				t.Fatalf("notifier: expected current revision %d, have %d", int64(i+1),
					ev.Current)
			}
			if ev.Created != int64(i+1) {
				t.Fatalf("notifier: expected created revision %d, have %d", int64(i+1),
					ev.Created)
			}
			if ev.Data.(int) != i {
				t.Fatalf("notifier: expected value %d, have %d", i, ev.Data.(int))
			}
		}
		close(done)
	}()

	tx = db.Txn()
	for i := 1; i <= count; i++ {
		tx.Put(key, i, false)
	}
	tx.Commit()

	n.Cancel()
	<-done
}

func TestNotifierCancelByUser(t *testing.T) {
	s := &stream{}
	n := s.Register()

	n.Cancel()
	if s.num != 0 {
		t.Fatalf("stream: expected notifier count zero, have %d", s.num)
	}
	if s.running {
		t.Fatalf("stream: stream not closed")
	}
	if s.notifiers != nil {
		t.Fatalf("stream: expected <nil> map: have %v", s.notifiers)
	}

	if n.id != -1 {
		t.Fatalf("notifier: expected zero id, have %d", n.id)
	}

	n.Cancel()
	if n.id != -1 {
		t.Fatalf("notifier: expected zero id, have %d", n.id)
	}
}

func TestNotifierCanceld(t *testing.T) {
	s := &stream{}
	n := s.Register()

	s.Cancel()
	if s.num != 0 {
		t.Fatalf("stream: expected notifier count zero, have %d", s.num)
	}
	if s.running {
		t.Fatalf("stream: stream not closed")
	}
	if s.notifiers != nil {
		t.Fatalf("stream: expected <nil> map: have %v", s.notifiers)
	}

	if n.id != -1 {
		t.Fatalf("notifier: expected zero id, have %d", n.id)
	}

	n.Cancel()
	if n.id != -1 {
		t.Fatalf("notifier: expected zero id, have %d", n.id)
	}
}
