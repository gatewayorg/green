package cache

import "github.com/VictoriaMetrics/fastcache"

type local struct {
	c *fastcache.Cache
}

func New(maxBytes int) ICache {
	return &local{
		c: fastcache.New(maxBytes),
	}
}

func (l *local) Get(dst, key []byte) []byte {
	return l.c.Get(dst, key)
}

func (l *local) Set(key, val []byte) {
	l.c.Set(key, val)
}

func (l *local) Del(key []byte) {
	l.c.Del(key)
}

func (l *local) Exists(key []byte) bool {
	return l.c.Has(key)
}

func (l *local) Reset() {
	l.c.Reset()
}
