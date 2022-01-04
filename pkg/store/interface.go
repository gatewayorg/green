package store

type IKV interface {
	Get(key []byte) ([]byte, error)
	Set(key, val []byte) error
	Del(key []byte) error
	BatchGet(key ...[]byte) ([][]byte, error)
	RangeGet(start, end []byte) ([][]byte, error)
	NewBatch() IBatch
}

type IBatch interface {
	Set(key, value []byte) error
	Del(key []byte) error
	Commit() error
	Close() error
}
