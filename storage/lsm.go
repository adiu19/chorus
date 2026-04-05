package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// LSM is a wrapper over our data store
type LSM struct {
	MemTable atomic.Pointer[SkipList]
	Manifest *LSMManifest

	ImmutableMemTable atomic.Pointer[SkipList] // a skiplist is marked as immutable when it no longer wants to accept writes and wants to be "flushed"
}

// LSMManifest captures the metadata required to load the data references
type LSMManifest struct {
	RootPath         string
	OrderedTableRefs []SSTable
	NextSeq          int64
}

// SSTable represents a sorted collection of KVs on disc
type SSTable struct {
	PathRef     string
	MinKey      []byte // for later read optimizations
	MaxKey      []byte // for later read optimizations
	CreationSeq int64  // used for sorting references
}

// KVEntry represents one KV in our SSTable
type KVEntry struct {
	Tombstone bool
	KeySize   uint8 // byte array transformed into uint8 on disk load
	Key       []byte
	ValSize   uint16 // byte array transformed into uint16. on disk load
	Val       []byte
}

// Flush persists the current memtable on disc
func (lsm *LSM) Flush() error {
	// Swap memtable: old becomes immutable, new accepts writes
	old := lsm.MemTable.Load()
	lsm.ImmutableMemTable.Store(old)
	lsm.MemTable.Store(NewSkipList())

	// Build file path
	seq := lsm.Manifest.NextSeq
	lsm.Manifest.NextSeq++
	filename := fmt.Sprintf("sstable_%06d.dat", seq)
	path := filepath.Join(lsm.Manifest.RootPath, filename)

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
	for node != nil {
		// Tombstone: 1 byte
		var tomb byte = 0
		if err := w.WriteByte(tomb); err != nil {
			return fmt.Errorf("flush: write tombstone: %w", err)
		}

		// Key length: 1 byte (uint8)
		if err := w.WriteByte(uint8(len(node.Key))); err != nil {
			return fmt.Errorf("flush: write key len: %w", err)
		}

		// Key
		if _, err := w.Write(node.Key); err != nil {
			return fmt.Errorf("flush: write key: %w", err)
		}

		// Value length: 2 bytes (uint16, big-endian)
		valLenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(valLenBuf, uint16(len(node.Value)))
		if _, err := w.Write(valLenBuf); err != nil {
			return fmt.Errorf("flush: write val len: %w", err)
		}

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
	}
	lsm.Manifest.OrderedTableRefs = append([]SSTable{sstable}, lsm.Manifest.OrderedTableRefs...)

	// Clear immutable memtable
	lsm.ImmutableMemTable.Store(nil)

	return nil
}
