package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// LSM is a wrapper over our data store
type LSM struct {
	memTable atomic.Pointer[SkipList]
	manifest *LSMManifest

	immutableMemTable atomic.Pointer[SkipList] // a skiplist is marked as immutable when it no longer wants to accept writes and wants to be "flushed"
	mu                sync.Mutex
}

// LSMManifest captures the metadata required to load the data references
type LSMManifest struct {
	rootPath            string
	orderedTableRefs    []SSTable
	nextSeq             int64
	maxBytesBeforeFlush int
}

// SSTable represents a sorted collection of KVs on disc
type SSTable struct {
	PathRef     string
	MinKey      []byte // for later read optimizations
	MaxKey      []byte // for later read optimizations
	CreationSeq int64  // used for sorting references
	SizeInBytes int
}

// KVEntry represents one KV in our SSTable
type KVEntry struct {
	Tombstone bool
	KeySize   uint8 // byte array transformed into uint8 on disk load
	Key       []byte
	ValSize   uint16 // byte array transformed into uint16. on disk load
	Val       []byte
}

// NewLSM inits a new LSM
func NewLSM(rootPath string) (*LSM, error) {
	nextSeq, err := loadOrInitManifest(rootPath)
	if err != nil {
		return nil, fmt.Errorf("lsm init: %w", err)
	}

	res := &LSM{
		manifest: &LSMManifest{
			rootPath:            rootPath,
			orderedTableRefs:    []SSTable{},
			nextSeq:             nextSeq,
			maxBytesBeforeFlush: 500 * 1024 * 1024, //500MB
		},
	}
	res.memTable.Store(NewSkipList())
	return res, nil
}

const manifestFile = "manifest.mf"

// loadOrInitManifest reads the manifest file from rootPath.
// If it exists, parses and returns nextSeq.
// If it doesn't exist, creates it with nextSeq=1 and returns 1.
func loadOrInitManifest(rootPath string) (int64, error) {
	path := filepath.Join(rootPath, manifestFile)

	data, err := os.ReadFile(path)
	if err == nil {
		// File exists — parse nextSeq
		var seq int64
		_, parseErr := fmt.Sscanf(string(data), "nextSeq=%d", &seq)
		if parseErr != nil {
			return 0, fmt.Errorf("parse manifest: %w", parseErr)
		}
		return seq, nil
	}

	if !os.IsNotExist(err) {
		return 0, fmt.Errorf("read manifest: %w", err)
	}

	// File doesn't exist — create new store
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return 0, fmt.Errorf("create root path: %w", err)
	}
	if err := os.WriteFile(path, []byte("nextSeq=1"), 0644); err != nil {
		return 0, fmt.Errorf("write manifest: %w", err)
	}
	return 1, nil
}

// Get fetches a key from the LSM
func (lsm *LSM) Get(key []byte) ([]byte, error) {
	if val := lsm.memTable.Load().Get(key); val != nil {
		return val, nil
	}

	if immutable := lsm.immutableMemTable.Load(); immutable != nil {
		if val := immutable.Get(key); val != nil {
			return val, nil
		}
	}

	lvl3, err := lsm.scanSSTablesForKey(key)
	if err != nil {
		return nil, err
	}

	if lvl3 != nil {
		return lvl3, nil
	}

	return nil, nil
}

// assumes that SSTable references in LSM are sorted with latest first
func (lsm *LSM) scanSSTablesForKey(key []byte) ([]byte, error) {
	tables := lsm.manifest.orderedTableRefs
	for _, table := range tables {
		cmpWithMin := bytes.Compare(key, table.MinKey)
		cmpWithMax := bytes.Compare(key, table.MaxKey)
		if cmpWithMin >= 0 && cmpWithMax <= 0 {
			val, found, err := lsm.readFromSSTable(key, table.PathRef)
			if err != nil {
				return nil, err
			}
			if found {
				return val, nil // val is nil if tombstone
			}
		}
	}

	return nil, nil
}

// readFromSSTable scans an SSTable file for a key.
// Returns (value, true, nil) if found, (nil, true, nil) if tombstoned, (nil, false, nil) if not present.
func (lsm *LSM) readFromSSTable(key []byte, path string) ([]byte, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, fmt.Errorf("read sstable: open: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

	for {
		// Tombstone: 1 byte
		tomb, err := r.ReadByte()
		if err != nil {
			break // EOF or error — key not in this file
		}

		// Key length: 1 byte
		keyLen, err := r.ReadByte()
		if err != nil {
			return nil, false, fmt.Errorf("read sstable: read key len: %w", err)
		}

		// Key
		entryKey := make([]byte, keyLen)
		if _, err := io.ReadFull(r, entryKey); err != nil {
			return nil, false, fmt.Errorf("read sstable: read key: %w", err)
		}

		// Value length: 2 bytes (big-endian)
		valLenBuf := make([]byte, 2)
		if _, err := io.ReadFull(r, valLenBuf); err != nil {
			return nil, false, fmt.Errorf("read sstable: read val len: %w", err)
		}
		valLen := binary.BigEndian.Uint16(valLenBuf)

		// Check if this is the key we want
		if bytes.Equal(entryKey, key) {
			if tomb == 1 {
				return nil, true, nil // tombstone — key was deleted
			}
			val := make([]byte, valLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return nil, false, fmt.Errorf("read sstable: read value: %w", err)
			}
			return val, true, nil
		}

		// Not our key — skip past the value
		if _, err := r.Discard(int(valLen)); err != nil {
			return nil, false, fmt.Errorf("read sstable: skip value: %w", err)
		}
	}

	return nil, false, nil
}

// Flush persists the current memtable on disc
func (lsm *LSM) Flush() error {
	// Swap memtable: old becomes immutable, new accepts writes
	old := lsm.memTable.Load()
	lsm.immutableMemTable.Store(old)
	lsm.memTable.Store(NewSkipList())

	// Build file path
	seq := lsm.manifest.nextSeq
	lsm.manifest.nextSeq++
	filename := fmt.Sprintf("sstable_%06d.dat", seq)
	path := filepath.Join(lsm.manifest.rootPath, filename)

	// Create file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("flush: create file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	// Walk level 0 of the skiplist (sorted order), write each entry
	var minKey, maxKey []byte
	node := old.Head.Forward[0]
	sizeInBytes := 0
	for node != nil {
		// Tombstone: 1 byte
		var tomb byte = node.Tombstone
		sizeInBytes++
		if err := w.WriteByte(tomb); err != nil {
			return fmt.Errorf("flush: write tombstone: %w", err)
		}

		// Key length: 1 byte (uint8)
		if err := w.WriteByte(uint8(len(node.Key))); err != nil {
			return fmt.Errorf("flush: write key len: %w", err)
		}

		sizeInBytes++

		// Key
		if _, err := w.Write(node.Key); err != nil {
			return fmt.Errorf("flush: write key: %w", err)
		}

		sizeInBytes += len(node.Key)

		// Value length: 2 bytes (uint16, big-endian)
		valLenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(valLenBuf, uint16(len(node.Value)))
		if _, err := w.Write(valLenBuf); err != nil {
			return fmt.Errorf("flush: write val len: %w", err)
		}

		sizeInBytes += 2 + len(node.Value)

		// Value
		if _, err := w.Write(node.Value); err != nil {
			return fmt.Errorf("flush: write value: %w", err)
		}

		// Track min/max keys
		if minKey == nil {
			minKey = node.Key
		}
		maxKey = node.Key

		node = node.Forward[0]
	}

	// Flush buffer to OS
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush: bufio flush: %w", err)
	}

	// Fsync to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("flush: fsync: %w", err)
	}

	// Update manifest: prepend (newest first)
	sstable := SSTable{
		PathRef:     path,
		MinKey:      minKey,
		MaxKey:      maxKey,
		CreationSeq: seq,
		SizeInBytes: sizeInBytes,
	}
	lsm.manifest.orderedTableRefs = append([]SSTable{sstable}, lsm.manifest.orderedTableRefs...)

	// Persist updated nextSeq to manifest file
	manifestPath := filepath.Join(lsm.manifest.rootPath, manifestFile)
	if err := os.WriteFile(manifestPath, []byte(fmt.Sprintf("nextSeq=%d", lsm.manifest.nextSeq)), 0644); err != nil {
		return fmt.Errorf("flush: update manifest: %w", err)
	}

	// Clear immutable memtable
	lsm.immutableMemTable.Store(nil)

	return nil
}

// Insert adds a new KV into the LSM
func (lsm *LSM) Insert(key []byte, value []byte) error {
	defer lsm.checkAndTriggerAutoFlush()
	return lsm.memTable.Load().Insert(key, value)
}

// Delete marks a key for deletion
func (lsm *LSM) Delete(key []byte) error {
	defer lsm.checkAndTriggerAutoFlush()
	return lsm.memTable.Load().InsertWithTombstone(key)
}

// checkAndTriggerAutoFlush spawns a separate goroutine that triggers auto flush if size permits. a lock is used to prevent concurrent writes to trigger empty sstables on flush
func (lsm *LSM) checkAndTriggerAutoFlush() {
	if lsm.memTable.Load().SizeInBytes >= lsm.manifest.maxBytesBeforeFlush {
		go func() {
			if lsm.mu.TryLock() {
				err := lsm.Flush()
				if err != nil {
					fmt.Println("flush failed...", err)
				}
				lsm.mu.Unlock()
			}
		}()
	}
}
