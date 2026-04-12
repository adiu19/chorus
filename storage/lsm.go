package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LSM is a wrapper over our data store
type LSM struct {
	memTable atomic.Pointer[SkipList]
	manifest *LSMManifest
	wal      *WAL

	immutableMemTable atomic.Pointer[SkipList] // a skiplist is marked as immutable when it no longer wants to accept writes and wants to be "flushed"
	mu                sync.Mutex
	levels            [][]SSTable
	compactTicker     *time.Ticker
	done              chan bool
}

// LSMManifest captures the metadata required to load the data references
type LSMManifest struct {
	rootPath            string
	orderedTableRefs    []SSTable
	nextSeq             int64
	maxBytesBeforeFlush int
}

// SSTable represents a sorted collection of KVs on disk
type SSTable struct {
	PathRef     string
	MinKey      []byte // for later read optimizations
	MaxKey      []byte // for later read optimizations
	CreationSeq int64  // used for sorting references
	SizeInBytes int
	BF          *BloomFilter
}

// KVEntry represents one KV in our SSTable
type KVEntry struct {
	Tombstone byte
	KeySize   uint8 // byte array transformed into uint8 on disk load
	Key       []byte
	ValSize   uint16 // byte array transformed into uint16. on disk load
	Val       []byte
}

// NewLSM inits a new LSM
func NewLSM(rootPath string) (*LSM, error) {
	sstableDir := filepath.Join(rootPath, "sstables")

	nextSeq, tables, err := loadOrInitManifest(sstableDir)
	if err != nil {
		return nil, fmt.Errorf("lsm init: %w", err)
	}

	done := make(chan bool)
	w, err := newWAL(rootPath, "wal", done)
	if err != nil {
		return nil, err
	}
	res := &LSM{
		wal: w,
		manifest: &LSMManifest{
			rootPath:            sstableDir,
			orderedTableRefs:    tables,
			nextSeq:             nextSeq,
			maxBytesBeforeFlush: 500 * 1024 * 1024, //500MB
		},
		compactTicker: time.NewTicker(60 * time.Second),
		done:          done,
	}
	res.memTable.Store(NewSkipList())
	if err := res.RecoverExistingWALs(); err != nil {
		return nil, fmt.Errorf("lsm init: %w", err)
	}
	go res.Compact()
	return res, nil
}

// Close closes all resources within the LSM
func (lsm *LSM) Close() {
	close(lsm.done)
}

const manifestFile = "manifest.mf"

// loadOrInitManifest reads the manifest file from rootPath.
// If it exists, parses SSTable entries and derives nextSeq from the last entry.
// If it doesn't exist, creates an empty manifest file.
func loadOrInitManifest(rootPath string) (int64, []SSTable, error) {
	path := filepath.Join(rootPath, manifestFile)

	data, err := os.ReadFile(path)
	if err == nil {
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		var tables []SSTable
		for _, line := range lines {
			if line == "" {
				continue
			}
			sst, parseErr := parseSSTableLine(line)
			if parseErr != nil {
				return 0, nil, fmt.Errorf("parse manifest sstable: %w", parseErr)
			}
			bf, bfErr := LoadBloomFilterFromSSTable(filepath.Join(rootPath, sst.PathRef))
			if bfErr != nil {
				return 0, nil, fmt.Errorf("load bloom filter: %w", bfErr)
			}
			sst.BF = bf
			tables = append(tables, sst)
		}

		// Derive nextSeq from the last (newest) entry
		var nextSeq int64 = 1
		if len(tables) > 0 {
			nextSeq = tables[len(tables)-1].CreationSeq + 1
		}

		// Reverse so newest is first (last appended = newest)
		for i, j := 0, len(tables)-1; i < j; i, j = i+1, j-1 {
			tables[i], tables[j] = tables[j], tables[i]
		}

		return nextSeq, tables, nil
	}

	if !os.IsNotExist(err) {
		return 0, nil, fmt.Errorf("read manifest: %w", err)
	}

	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return 0, nil, fmt.Errorf("create root path: %w", err)
	}
	if err := os.WriteFile(path, []byte(""), 0644); err != nil {
		return 0, nil, fmt.Errorf("write manifest: %w", err)
	}
	return 1, nil, nil
}

// parseSSTableLine parses a line like:
// sstable_000001.dat,seq=1,size=4096,minKey=<base62>,maxKey=<base62>
func parseSSTableLine(line string) (SSTable, error) {
	parts := strings.Split(line, ",")
	if len(parts) != 5 {
		return SSTable{}, fmt.Errorf("expected 5 fields, got %d: %s", len(parts), line)
	}

	filename := parts[0]

	var seq int64
	if _, err := fmt.Sscanf(parts[1], "seq=%d", &seq); err != nil {
		return SSTable{}, fmt.Errorf("parse seq: %w", err)
	}

	var size int
	if _, err := fmt.Sscanf(parts[2], "size=%d", &size); err != nil {
		return SSTable{}, fmt.Errorf("parse size: %w", err)
	}

	var minKeyB62, maxKeyB62 string
	if _, err := fmt.Sscanf(parts[3], "minKey=%s", &minKeyB62); err != nil {
		return SSTable{}, fmt.Errorf("parse minKey: %w", err)
	}
	if _, err := fmt.Sscanf(parts[4], "maxKey=%s", &maxKeyB62); err != nil {
		return SSTable{}, fmt.Errorf("parse maxKey: %w", err)
	}

	return SSTable{
		PathRef:     filename,
		CreationSeq: seq,
		SizeInBytes: size,
		MinKey:      Base62Decode(minKeyB62),
		MaxKey:      Base62Decode(maxKeyB62),
	}, nil
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
		if !table.BF.Exists(key) {
			// if our bloom filter says the key doesn't exist, we trust and move onto the next one.
			continue
		}
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

	// skip bloom filter — already loaded in memory from manifest init
	if _, err := io.ReadFull(r, make([]byte, bloomFilterSize)); err != nil {
		return nil, false, fmt.Errorf("read sstable: skip bloom filter: %w", err)
	}

	for {
		entry, err := ReadKVEntry(r)
		if err != nil {
			return nil, false, fmt.Errorf("read sstable: %w", err)
		}
		if entry == nil {
			break // EOF
		}

		if bytes.Equal(entry.Key, key) {
			if entry.Tombstone == 1 {
				return nil, true, nil // tombstone — key was deleted
			}
			return entry.Val, true, nil
		}
	}

	return nil, false, nil
}

// writeSkipListToSSTable writes a skiplist to a new SSTable file, fsyncs it, and updates the manifest.
// Reused by both Flush (normal operation) and recovery (WAL replay).
func (lsm *LSM) writeSkipListToSSTable(sl *SkipList) error {
	// Build file path
	seq := lsm.manifest.nextSeq
	lsm.manifest.nextSeq++
	filename := fmt.Sprintf("sstable_%06d.dat", seq)
	path := filepath.Join(lsm.manifest.rootPath, filename)

	// Create file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("write sstable: create file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	// Pass 1: build bloom filter from skiplist keys, write it as the first 8KB
	bf := sl.BuildBloomFilter()
	if _, err := w.Write(bf.BitMap); err != nil {
		return fmt.Errorf("write sstable: write bloom filter: %w", err)
	}

	// Pass 2: walk level 0 of the skiplist (sorted order), write each entry
	var minKey, maxKey []byte
	node := sl.Head.Forward[0]
	sizeInBytes := 0
	for node != nil {
		n, err := WriteKVEntry(w, node.Key, node.Value, node.Tombstone)
		if err != nil {
			return fmt.Errorf("write sstable: %w", err)
		}
		sizeInBytes += n

		// Track min/max keys
		if minKey == nil {
			minKey = node.Key
		}
		maxKey = node.Key

		node = node.Forward[0]
	}

	// Flush buffer to OS
	if err := w.Flush(); err != nil {
		return fmt.Errorf("write sstable: bufio flush: %w", err)
	}

	// Fsync to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("write sstable: fsync: %w", err)
	}

	// Update manifest in memory: prepend (newest first)
	sstable := SSTable{
		PathRef:     path,
		MinKey:      minKey,
		MaxKey:      maxKey,
		CreationSeq: seq,
		SizeInBytes: sizeInBytes,
		BF:          bf,
	}
	lsm.manifest.orderedTableRefs = append([]SSTable{sstable}, lsm.manifest.orderedTableRefs...)

	// Append SSTable entry to manifest file
	if err := lsm.persistManifest(sstable); err != nil {
		return fmt.Errorf("write sstable: update manifest: %w", err)
	}

	return nil
}

// Flush persists the current memtable on disk
func (lsm *LSM) Flush() error {
	lsm.wal.switchReference() // swap WAL references
	// Swap memtable: old becomes immutable, new accepts writes
	old := lsm.memTable.Load()
	lsm.immutableMemTable.Store(old)
	lsm.memTable.Store(NewSkipList())

	if err := lsm.writeSkipListToSSTable(old); err != nil {
		return err
	}

	// Clear immutable memtable
	lsm.immutableMemTable.Store(nil)

	return nil
}

// RecoverExistingWALs is triggered on process startup to ensure that any WALs that are lying around are processed
// before we make our engine available.
func (lsm *LSM) RecoverExistingWALs() error {
	// 1. read all old WAL files
	entries, paths, err := lsm.wal.readAllEntries()
	if err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	if len(entries) == 0 {
		return nil // nothing to recover
	}

	// 2. build skiplist from WAL entries
	sl := NewSkipList()
	for _, entry := range entries {
		if entry.Tombstone == 1 {
			sl.InsertWithTombstone(entry.Key)
		} else {
			sl.Insert(entry.Key, entry.Val)
		}
	}

	// 3. write skiplist to SSTable (fsync + manifest update)
	if err := lsm.writeSkipListToSSTable(sl); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	// 4. delete old WAL files — data is now durable in SSTable
	if err := lsm.wal.deleteFiles(paths); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	return nil
}

// persistManifest appends one SSTable entry to the manifest file
func (lsm *LSM) persistManifest(sst SSTable) error {
	path := filepath.Join(lsm.manifest.rootPath, manifestFile)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()

	line := fmt.Sprintf("%s,seq=%d,size=%d,minKey=%s,maxKey=%s\n",
		filepath.Base(sst.PathRef),
		sst.CreationSeq,
		sst.SizeInBytes,
		Base62Encode(sst.MinKey),
		Base62Encode(sst.MaxKey),
	)

	if _, err := f.WriteString(line); err != nil {
		return fmt.Errorf("write manifest entry: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync manifest: %w", err)
	}

	return nil
}

// Insert adds a new KV into the LSM
func (lsm *LSM) Insert(key []byte, value []byte) error {
	defer lsm.checkAndTriggerAutoFlush()
	if err := lsm.wal.write(key, value, 0); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return lsm.memTable.Load().Insert(key, value)
}

// Delete marks a key for deletion
func (lsm *LSM) Delete(key []byte) error {
	defer lsm.checkAndTriggerAutoFlush()
	if err := lsm.wal.write(key, []byte{}, 1); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
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

// Compact manages the compaction our sstables
func (lsm *LSM) Compact() {
	// 1. start a goroutine
	// 2. goroutine wakes every 60 seconds (via ticker), checks number of ss tables at L0
	// 3. if count of sstables exceed a threshold, it triggers a cascading effect where we first compact L0 into L1
	// 4. if L1 size exceeds its threshold, we move down to L2. same for L3
	for {
		select {
		case <-lsm.compactTicker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("compactor panicked:", r)
					}
				}()
				lsm.mergeSSTables()
			}()
		case <-lsm.done:
			lsm.compactTicker.Stop()
			return

		}

	}
}

func (lsm *LSM) mergeSSTables() {
	if len(lsm.levels[0]) > 100 { // if the number of ss tables in level 0 is greater than 100

	}
}
