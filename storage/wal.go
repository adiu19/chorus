package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const fnamePrefix = "wal-"

// WAL represents our interface to interact with write-ahead-log for durability
type WAL struct {
	dir    string
	file   *os.File
	writer *bufio.Writer
	// TODO: deprecate locks
	// caller blocks until fsync: Insert adds to the buffer and waits until the batch is fsynced before returning. Durability is the same as per-write fsync. Latency per write goes up (we wait for the batch window),
	// but throughput goes up massively (one fsync for N writes).
	mu     sync.Mutex
	buffer atomic.Pointer[WALBuffer]
	ticker *time.Ticker
	done   chan bool
}

// WALBuffer is intended to buffer write intents to avoid excessive fsync calls and improve throughput
// since WAL writes sit on the critical path
type WALBuffer struct {
	// NOTE: appending to the slice via atomic pointer is not safe — two goroutines can Load(), append, Store()
	// and one append is lost. The atomic only guarantees a clean swap in bufferedWrite(). Appends need mutex protection.
	entries  []KVEntry
	activeCh chan bool
}

func newWAL(base string, dir string, done chan bool) (*WAL, error) {
	fullPath := filepath.Join(base, dir)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("wal init: mkdir: %w", err)
	}

	w := &WAL{
		dir:    fullPath,
		ticker: time.NewTicker(10 * time.Millisecond),
		done:   done,
	}
	w.buffer.Store(newBuffer())

	if err := w.openNew(); err != nil {
		return nil, fmt.Errorf("wal init: %w", err)
	}

	go w.bufferedWrite()
	return w, nil
}

func newBuffer() *WALBuffer {
	b := WALBuffer{
		entries:  []KVEntry{},
		activeCh: make(chan bool),
	}

	return &b
}

func (w *WAL) bufferedWrite() {
	for {
		select {
		case <-w.done:
			return
		case <-w.ticker.C:
			// part 0: create a new buffer and channel
			upcoming := WALBuffer{
				entries:  []KVEntry{},
				activeCh: make(chan bool),
			}
			prev := w.buffer.Swap(&upcoming)
			if len(prev.entries) > 0 {
				// write all buffered entries to disk
				for _, entry := range prev.entries {
					if _, err := WriteKVEntry(w.writer, entry.Key, entry.Val, entry.Tombstone); err != nil {
						fmt.Println("wal buffered write failed:", err)
						break
					}
				}
				// flush bufio buffer to OS, then fsync to disk
				// TODO: errors here are printed but writers still get unblocked via close(activeCh)
				// they think their batch succeeded. To fix: store the error on WALBuffer, have writers
				// check it after <-ch and return it from write().
				if err := w.writer.Flush(); err != nil {
					fmt.Println("wal buffered flush failed:", err)
				}
				if err := w.file.Sync(); err != nil {
					fmt.Println("wal buffered fsync failed:", err)
				}
				// wake all writers waiting on this batch
				close(prev.activeCh)
			}
		}
	}
}

// openNew creates a new WAL file and sets it as the active writer
func (w *WAL) openNew() error {
	name := fnamePrefix + strconv.FormatInt(time.Now().UnixNano(), 10)
	path := filepath.Join(w.dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}

	w.file = f
	w.writer = bufio.NewWriter(f)
	return nil
}

func (w *WAL) write(key []byte, value []byte, tombstone byte) error {
	// lock to protect concurrent appends to the same buffer
	w.mu.Lock()
	current := w.buffer.Load()
	current.entries = append(current.entries, KVEntry{
		Key:       key,
		Val:       value,
		Tombstone: tombstone,
	})
	ch := current.activeCh // grab channel reference before unlocking
	w.mu.Unlock()

	// block until this batch is fsynced by the ticker goroutine
	<-ch

	return nil
}

// switchReference closes the current WAL file and opens a new one.
// Returns the path of the old WAL file so the caller can delete it after flush completes.
func (w *WAL) switchReference() (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	oldPath := w.file.Name()

	// flush and close old file
	if err := w.writer.Flush(); err != nil {
		return "", fmt.Errorf("wal switch: flush old: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return "", fmt.Errorf("wal switch: fsync old: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return "", fmt.Errorf("wal switch: close old: %w", err)
	}

	// open new file for incoming writes
	if err := w.openNew(); err != nil {
		return "", fmt.Errorf("wal switch: %w", err)
	}

	return oldPath, nil
}

// readAllEntries reads all WAL files in the directory (sorted by epoch), parses entries, and returns them.
// Also returns the file paths so the caller can delete them after processing.
func (w *WAL) readAllEntries() ([]KVEntry, []string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, nil, fmt.Errorf("wal recovery: read dir: %w", err)
	}

	// collect and sort WAL files by name (epoch in name gives chronological order)
	var walFiles []string
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > len(fnamePrefix) && e.Name()[:len(fnamePrefix)] == fnamePrefix {
			// skip the current active WAL file
			if filepath.Join(w.dir, e.Name()) == w.file.Name() {
				continue
			}
			walFiles = append(walFiles, filepath.Join(w.dir, e.Name()))
		}
	}
	sort.Strings(walFiles)

	var result []KVEntry
	for _, path := range walFiles {
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, fmt.Errorf("wal recovery: open %s: %w", path, err)
		}

		r := bufio.NewReader(f)
		for {
			entry, err := ReadKVEntry(r)
			if err != nil {
				f.Close()
				return nil, nil, fmt.Errorf("wal recovery: read %s: %w", path, err)
			}
			if entry == nil {
				break // EOF
			}
			result = append(result, *entry)
		}
		f.Close()
	}

	return result, walFiles, nil
}

// deleteFiles removes the given file paths from disk
func (w *WAL) deleteFiles(paths []string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("wal cleanup: delete %s: %w", path, err)
		}
	}
	return nil
}
