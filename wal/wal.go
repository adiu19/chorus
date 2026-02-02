package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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

	// Initialize nextSeq from existing entries
	entries, err := w.ReadAll()
	if err != nil {
		w.file.Close()
		return nil, err
	}
	if len(entries) > 0 {
		w.nextSeq = entries[len(entries)-1].Seq + 1
	}

	// Read index.log if exists
	indexPath := filepath.Join(dir, "index.log")
	data, err := os.ReadFile(indexPath)
	if err == nil {
		if parsed, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			w.index = parsed
		}
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

func (w *WAL) ReadAll() ([]Entry, error) {
	// Seek to beginning of file
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries []Entry
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *WAL) SetIndex(seq int64) error {
	w.index = seq
	indexPath := filepath.Join(w.dir, "index.log")
	return os.WriteFile(indexPath, []byte(strconv.FormatInt(seq, 10)), 0644)
}

func (w *WAL) Index() int64 {
	return w.index
}

func (w *WAL) Append(tag, op, key string, value []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := Entry{Seq: w.nextSeq, Tag: tag, Op: op, Key: key, Value: value}
	w.nextSeq++

	data, _ := json.Marshal(entry)
	_, err := w.file.Write(append(data, '\n'))
	return entry.Seq, err
}
