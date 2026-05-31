package backend

import (
	"github.com/chorus/storage"
)

type StoreBackend struct {
	s storage.Store
}

func NewStoreBackend(s storage.Store) *StoreBackend {
	return &StoreBackend{
		s: s,
	}
}

func (sb *StoreBackend) Fetch(hash []byte) ([]byte, error) {
	res, err := sb.s.Get(hash)
	if err != nil || res == nil {
		return nil, ErrNotFound
	}

	return res, nil
}

func (sb *StoreBackend) Stat(hash []byte) (Info, error) {
	res, err := sb.Fetch(hash)
	if err != nil {
		return Info{}, err
	}
	return Info{Size: int64(len(res))}, nil
}

func (sb *StoreBackend) Has(hash []byte) bool {
	_, err := sb.Fetch(hash)
	if err != nil {
		return false
	}

	return true

}

func (sb *StoreBackend) Close() error {
	// no-op for this
	return nil
}

var _ Backend = (*StoreBackend)(nil)
