package storage

// Store is an interface for all our data needs
type Store interface {
	Insert(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}
