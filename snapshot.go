package db

import "github.com/azmodb/db/backend"

func (db *DB) Snapshot(backend backend.Backend) error { return nil }
func (db *DB) Reload(backend backend.Backend) error   { return nil }
