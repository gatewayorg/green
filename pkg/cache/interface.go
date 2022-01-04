package cache

type ICache interface {
	Get(dst, key []byte) []byte
	Set(key, val []byte)
	Del(key []byte)
	Exists(key []byte) bool
	Reset()
}
