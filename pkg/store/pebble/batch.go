package pebble

import "github.com/cockroachdb/pebble"

type batch struct {
	b *pebble.Batch
}

func (b *batch) Set(key, value []byte) error {
	return b.b.Set(key, value, pebble.NoSync)
}

func (b *batch) Del(key []byte) error {
	return b.b.Delete(key, pebble.NoSync)
}

func (b *batch) Commit() error {
	return b.b.Commit(pebble.NoSync)
}

func (b *batch) Close() error {
	return b.b.Close()
}
