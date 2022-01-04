package pebble

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/pkg/store"
)

type IHash interface {
	Sum64() uint64
	Write(b []byte) (int, error)
	WriteString(s string) (int, error)
	Reset()
}

type kvInst struct {
	db *pebble.DB
}

const (
	KV_NAME           = "kvdb"
	SHARE_MEMORY_SIZE = 536870912 // 512Mi
)

var (
	ERR_INTERNAL_KV = errors.New("kv internal error")
)

func New() store.IKV {
	kv := &kvInst{}
	cache := pebble.NewCache(SHARE_MEMORY_SIZE)
	db, err := pebble.Open(KV_NAME, &pebble.Options{
		MaxOpenFiles:             10000,
		MaxConcurrentCompactions: 4,
		Cache:                    cache,
	})
	if err != nil {
		panic(err)
	}
	kv.db = db
	return kv
}

func (k *kvInst) Get(key []byte) ([]byte, error) {
	snap := k.db.NewSnapshot()
	defer snap.Close()
	res, rio, err := snap.Get(key)
	if err != nil {
		log.Error(err)
		return nil, ERR_INTERNAL_KV
	}
	defer rio.Close()
	return res, nil
}

func (k *kvInst) Set(key, value []byte) error {
	return k.db.Set(key, value, pebble.Sync)
}

func (k *kvInst) Del(key []byte) error {
	return k.db.Delete(key, pebble.Sync)
}

func (k *kvInst) BatchGet(key ...[]byte) ([][]byte, error) {
	rs := make([][]byte, len(key), len(key))
	snap := k.db.NewSnapshot()
	defer snap.Close()
	for index, v := range key {
		rsp, ri, err := snap.Get(v)
		if err != nil {
			log.Error(err)
			continue
		}
		ri.Close()
		rs[index] = rsp
	}

	return rs, nil
}

func (k *kvInst) RangeGet(start, end []byte) ([][]byte, error) {
	o := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	snap := k.db.NewSnapshot()
	iter := snap.NewIter(o)
	defer func() {
		iter.Clone()
		snap.Close()
	}()
	rs := make([][]byte, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		rs = append(rs, iter.Value())
	}
	return rs, nil
}

func (k *kvInst) NewBatch() store.IBatch {
	return &batch{
		b: k.db.NewBatch(),
	}
}
