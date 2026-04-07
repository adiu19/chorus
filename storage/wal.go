package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
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
	mu sync.Mutex
}

func newWAL(base string, dir string) (*WAL, error) {
	fullPath := filepath.Join(base, dir)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("wal init: mkdir: %w", err)
	}

	w := &WAL{dir: fullPath}
	if err := w.openNew(); err != nil {
		return nil, fmt.Errorf("wal init: %w", err)
	}
	return w, nil
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
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := WriteKVEntry(w.writer, key, value, tombstone); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	// flush bufio buffer to OS, then fsync to disk
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal fsync: %w", err)
	}

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
