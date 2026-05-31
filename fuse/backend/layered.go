package backend

import (
	"errors"
	"log"

	"github.com/chorus/storage"
)

// LayeredBackend provides a staggered approach to fetch data
type LayeredBackend struct {
	local  storage.Store
	peer   *PeerBackend
	origin *RemoteKVServerBackend
}

// NewLayeredBackend instantiates a layered backend with an ordered list of backend preferences
func NewLayeredBackend(local storage.Store, peer *PeerBackend, origin *RemoteKVServerBackend) (*LayeredBackend, error) {
	return &LayeredBackend{
		local:  local,
		peer:   peer,
		origin: origin,
	}, nil
}

// Close cleans up the remote connection
func (sb *LayeredBackend) Close() error {
	var errToReturn1, errToReturn2 error
	errToReturn1 = sb.peer.Close()
	errToReturn2 = sb.origin.Close()
	return errors.Join(errToReturn1, errToReturn2)
}

// Fetch returns the data bytes against an input hash
func (sb *LayeredBackend) Fetch(hash []byte) ([]byte, error) {
	if res, err := sb.local.Get(hash); err == nil && res != nil {
		return res, nil
	}

	if res, err := sb.peer.Fetch(hash); err == nil {
		go sb.recordHit(hash, res)
		return res, nil
	}

	if res, err := sb.origin.Fetch(hash); err == nil {
		go sb.recordHit(hash, res)
		return res, nil
	}

	return nil, ErrNotFound
}

// recordHit caches the freshly-fetched bytes locally and announces to the cluster.
// Runs in a goroutine; failures are logged but don't affect the Fetch result.
func (sb *LayeredBackend) recordHit(hash, value []byte) {
	if err := sb.local.Insert(hash, value); err != nil {
		log.Printf("cache-fill: %v", err)
	}
	if err := sb.peer.Announce(hash); err != nil {
		log.Printf("announce: %v", err)
	}
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
