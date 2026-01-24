package store

type Store interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, bool)
	Delete(key string) error
}
