package wal

import (
	"os"
	"sync"
)

type Entry struct {
	Seq   int64  `json:"seq"`
	Tag   string `json:"tag"` // "primary" or "replica"
	Op    string `json:"op"`  // "put"
	Key   string `json:"key"`
	Value []byte `json:"value"` // JSON marshals as base64
}

type WAL struct {
	mu      sync.Mutex
	dir     string   // data/nodes/<node-id>
	file    *os.File // current.log handle
	nextSeq int64    // next sequence to assign
	index   int64    // last applied seq (-1 = none)
}
