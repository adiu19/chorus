package memstore

import (
	"sync"
)

// MemStore is a simple in-memory hash map
type MemStore struct {
	m  map[string][]byte
	mu sync.RWMutex
}

// NewMemStore instantiates our in-memory store
func NewMemStore() *MemStore {
	return &MemStore{
		m: make(map[string][]byte),
	}
}

// Insert adds a new KV to the store
func (ms *MemStore) Insert(key, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	strKey := string(key)
	ms.m[strKey] = value
	return nil
}

// Get fetches the value for a given key
func (ms *MemStore) Get(key []byte) ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	val, ok := ms.m[string(key)]
	if !ok {
		return nil, nil
	}

	return val, nil
}

// Delete remove a KV from our map
func (ms *MemStore) Delete(key []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.m, string(key))
	return nil
}
