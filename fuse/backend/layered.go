package backend

import (
	"fmt"

	"github.com/chorus/storage"
)

// LayeredBackend provides a staggered approach to fetch data
type LayeredBackend struct {
	cacheFill       storage.Store // optional — writes go here on hit from deeper layers; nil = no fill
	orderedBackends []Backend
}

// NewLayeredBackend instantiates a layered backend with an ordered list of backend preferences
func NewLayeredBackend(orderedBackends []Backend, cacheFill storage.Store) (*LayeredBackend, error) {
	return &LayeredBackend{
		cacheFill:       cacheFill,
		orderedBackends: orderedBackends,
	}, nil
}

// Close cleans up the remote connection
func (sb *LayeredBackend) Close() error {
	var errToReturn error
	for _, b := range sb.orderedBackends {
		err := b.Close()
		if err != nil {
			errToReturn = err
		}
	}

	return errToReturn
}

// Fetch returns the data bytes against an input hash
func (sb *LayeredBackend) Fetch(hash []byte) ([]byte, error) {
	for idx, backend := range sb.orderedBackends {
		res, err := backend.Fetch(hash)
		if err != nil {
			fmt.Printf("failed to fetch from backend %v", err)
		} else {
			if idx > 0 && sb.cacheFill != nil { // assumes that layer 0 is the in-memory backend
				cfErr := sb.cacheFill.Insert(hash, res)
				if cfErr != nil {
					fmt.Printf("failed to cache fill our in-memory layer : %v \n", cfErr)
				}
			}
			return res, nil
		}
	}

	return nil, ErrNotFound
}

func (sb *LayeredBackend) Stat(hash []byte) (Info, error) {
	res, err := sb.Fetch(hash)
	if err != nil {
		return Info{}, err
	}
	return Info{Size: int64(len(res))}, nil
}

func (sb *LayeredBackend) Has(hash []byte) bool {
	_, err := sb.Fetch(hash)
	if err != nil {
		return false
	}

	return true

}

var _ Backend = (*LayeredBackend)(nil)
