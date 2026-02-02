package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
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

func Open(dir string) (*WAL, error) {
	// Create dir if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Open/create current.log in append mode
	logPath := filepath.Join(dir, "current.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		dir:     dir,
		file:    file,
		nextSeq: 1,
		index:   -1,
	}

	// Register cleanup handler for graceful shutdown
	go w.registerCleanupHandler()

	return w, nil
}

func (w *WAL) registerCleanupHandler() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	w.Close()
}

func (w *WAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
