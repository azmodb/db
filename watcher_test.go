package db

import "testing"

func TestWatcherAddDelete(t *testing.T) {
	n := newNotifier()
	w1 := n.Add()
	w2 := n.Add()
	w3 := n.Add()
	if len(n.m) != 3 {
		t.Fatalf("add watcher: expected 3 watchers, have %d", len(n.m))
	}

	n.Delete(w1.ID())
	if len(n.m) != 2 {
		t.Fatalf("del watcher: expected 2 watchers, have %d", len(n.m))
	}
	n.Delete(w2.ID())
	if len(n.m) != 1 {
		t.Fatalf("del watcher: expected 1 watcher, have %d", len(n.m))
	}
	n.Delete(w3.ID())
	if len(n.m) != 0 {
		t.Fatalf("del watcher: expected 0 watchers, have %d", len(n.m))
	}
}

func TestNotifierClose(t *testing.T) {
	n := newNotifier()
	n.Close()
	n.Close()
	n.Close()

	n.Notify(nil, 0)
	n.Notify(nil, 0)
	n.Notify(nil, 0)
}
