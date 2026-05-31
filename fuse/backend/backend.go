package backend

import "errors"

type Info struct {
	Size int64
}

type Backend interface {
	Fetch(hash []byte) ([]byte, error) // full bytes for this hash
	Stat(hash []byte) (Info, error)    // metadata
	Has(hash []byte) bool              // presence check
	Close() error                      // close backend
}

var ErrNotFound = errors.New("not found")
