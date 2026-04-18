package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const capL0 = 4
const capL1 = 10
const capL2 = 100
const capL3 = 1000

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
	nextSeq             atomic.Int64
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

	if err := initSSTableDir(sstableDir); err != nil {
		return nil, fmt.Errorf("lsm init: %w", err)
	}

	levels, nextSeq, err := loadSSTablesFromDisk(sstableDir)
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
			maxBytesBeforeFlush: 500 * 1024 * 1024, //500MB
		},
		levels:        levels,
		compactTicker: time.NewTicker(60 * time.Second),
		done:          done,
	}
	res.manifest.nextSeq.Store(nextSeq)
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

const numLevels = 4

// initSSTableDir ensures the sstable root directory and level subdirectories exist.
func initSSTableDir(rootPath string) error {
	for i := 0; i < numLevels; i++ {
		levelDir := filepath.Join(rootPath, fmt.Sprintf("L%d", i))
		if err := os.MkdirAll(levelDir, 0755); err != nil {
			return fmt.Errorf("create level dir L%d: %w", i, err)
		}
	}
	return nil
}

// loadSSTablesFromDisk scans all level directories, loads valid SSTables (those with .stats),
// and deletes incomplete ones. Returns the levels and the next sequence number.
func loadSSTablesFromDisk(rootPath string) ([][]SSTable, int64, error) {
	levels := make([][]SSTable, numLevels)
	var maxSeq int64

	for i := 0; i < numLevels; i++ {
		levelDir := filepath.Join(rootPath, fmt.Sprintf("L%d", i))
		entries, err := os.ReadDir(levelDir)
		if err != nil {
			return nil, 0, fmt.Errorf("scan level L%d: %w", i, err)
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			sstDir := filepath.Join(levelDir, entry.Name())
			statsPath := filepath.Join(sstDir, ".stats")

			statsData, err := os.ReadFile(statsPath)
			if err != nil {
				// no .stats  - incomplete SSTable, clean it up
				os.RemoveAll(sstDir)
				continue
			}

			sst, err := parseStats(sstDir, statsData)
			if err != nil {
				os.RemoveAll(sstDir)
				continue
			}

			// load bloom filter
			bf, err := LoadBloomFilter(sstDir)
			if err != nil {
				os.RemoveAll(sstDir)
				continue
			}
			sst.BF = bf

			if sst.CreationSeq > maxSeq {
				maxSeq = sst.CreationSeq
			}

			levels[i] = append(levels[i], sst)
		}

		// sort each level by CreationSeq (newest first)
		sort.Slice(levels[i], func(a, b int) bool {
			return levels[i][a].CreationSeq > levels[i][b].CreationSeq
		})
	}

	return levels, maxSeq + 1, nil
}

// parseStats parses a .stats file into an SSTable struct.
// Format:
//
//	minKey=<base62>
//	maxKey=<base62>
//	size=<int>
//	seq=<int>
func parseStats(sstDir string, data []byte) (SSTable, error) {
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 4 {
		return SSTable{}, fmt.Errorf("parse stats: expected 4 lines, got %d", len(lines))
	}

	var minKeyB62, maxKeyB62 string
	var size int
	var seq int64

	if _, err := fmt.Sscanf(lines[0], "minKey=%s", &minKeyB62); err != nil {
		return SSTable{}, fmt.Errorf("parse stats: minKey: %w", err)
	}
	if _, err := fmt.Sscanf(lines[1], "maxKey=%s", &maxKeyB62); err != nil {
		return SSTable{}, fmt.Errorf("parse stats: maxKey: %w", err)
	}
	if _, err := fmt.Sscanf(lines[2], "size=%d", &size); err != nil {
		return SSTable{}, fmt.Errorf("parse stats: size: %w", err)
	}
	if _, err := fmt.Sscanf(lines[3], "seq=%d", &seq); err != nil {
		return SSTable{}, fmt.Errorf("parse stats: seq: %w", err)
	}

	return SSTable{
		PathRef:     sstDir,
		MinKey:      Base62Decode(minKeyB62),
		MaxKey:      Base62Decode(maxKeyB62),
		SizeInBytes: size,
		CreationSeq: seq,
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

// scanSSTablesForKey searches levels in order: L0 (check all, newest first),
// then L1, L2, L3 (non-overlapping  - at most one SSTable per level can match).
func (lsm *LSM) scanSSTablesForKey(key []byte) ([]byte, error) {
	for level := 0; level < numLevels; level++ {
		for _, table := range lsm.levels[level] {
			if !table.BF.Exists(key) {
				continue
			}
			if bytes.Compare(key, table.MinKey) < 0 || bytes.Compare(key, table.MaxKey) > 0 {
				continue
			}
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
func (lsm *LSM) readFromSSTable(key []byte, sstableDir string) ([]byte, bool, error) {
	dataPath := filepath.Join(sstableDir, ".data")
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, false, fmt.Errorf("read sstable: open: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

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
				return nil, true, nil // tombstone  - key was deleted
			}
			return entry.Val, true, nil
		}
	}

	return nil, false, nil
}

// writeSkipListToSSTable writes a skiplist to a new SSTable directory with three files:
//   - .data   - KV entries in binary format
//   - .bloom  - bloom filter bitmap
//   - .stats  - min/max keys (written last as commit marker)
//
// If .stats is missing, the SSTable is considered incomplete and skipped on startup.
// Reused by both Flush (normal operation), recovery (WAL replay), and compaction.
func (lsm *LSM) writeSkipListToSSTable(sl *SkipList, level int) error {
	seq := lsm.manifest.nextSeq.Load()
	lsm.manifest.nextSeq.Add(1)
	dirname := fmt.Sprintf("sstable_%06d", seq)
	dirPath := filepath.Join(lsm.manifest.rootPath, fmt.Sprintf("L%d", level), dirname)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("write sstable: mkdir: %w", err)
	}

	// 1. Write .bloom
	bf := sl.BuildBloomFilter()
	bloomPath := filepath.Join(dirPath, ".bloom")
	if err := writeAndSync(bloomPath, bf.BitMap); err != nil {
		return fmt.Errorf("write sstable: bloom: %w", err)
	}

	// 2. Write .data  - walk level 0 of the skiplist (sorted order)
	dataPath := filepath.Join(dirPath, ".data")
	dataFile, err := os.Create(dataPath)
	if err != nil {
		return fmt.Errorf("write sstable: create data: %w", err)
	}

	w := bufio.NewWriter(dataFile)
	var minKey, maxKey []byte
	node := sl.Head.Forward[0]
	sizeInBytes := 0
	for node != nil {
		n, err := WriteKVEntry(w, node.Key, node.Value, node.Tombstone)
		if err != nil {
			dataFile.Close()
			return fmt.Errorf("write sstable: data: %w", err)
		}
		sizeInBytes += n

		if minKey == nil {
			minKey = node.Key
		}
		maxKey = node.Key
		node = node.Forward[0]
	}

	if err := w.Flush(); err != nil {
		dataFile.Close()
		return fmt.Errorf("write sstable: data flush: %w", err)
	}
	if err := dataFile.Sync(); err != nil {
		dataFile.Close()
		return fmt.Errorf("write sstable: data fsync: %w", err)
	}
	dataFile.Close()

	// 3. Write .stats last  - commit marker
	statsPath := filepath.Join(dirPath, ".stats")
	statsContent := fmt.Sprintf("minKey=%s\nmaxKey=%s\nsize=%d\nseq=%d\n",
		Base62Encode(minKey),
		Base62Encode(maxKey),
		sizeInBytes,
		seq,
	)
	if err := writeAndSync(statsPath, []byte(statsContent)); err != nil {
		return fmt.Errorf("write sstable: stats: %w", err)
	}

	// Update in-memory state: prepend (newest first)
	sstable := SSTable{
		PathRef:     dirPath,
		MinKey:      minKey,
		MaxKey:      maxKey,
		CreationSeq: seq,
		SizeInBytes: sizeInBytes,
		BF:          bf,
	}
	lsm.levels[level] = append([]SSTable{sstable}, lsm.levels[level]...)

	return nil
}

// writeAndSync creates a file, writes data, fsyncs, and closes.
func writeAndSync(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Flush persists the current memtable on disk
func (lsm *LSM) Flush() error {
	lsm.wal.switchReference() // swap WAL references
	// Swap memtable: old becomes immutable, new accepts writes
	old := lsm.memTable.Load()
	lsm.immutableMemTable.Store(old)
	lsm.memTable.Store(NewSkipList())

	if err := lsm.writeSkipListToSSTable(old, 0); err != nil {
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
	if err := lsm.writeSkipListToSSTable(sl, 0); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	// 4. delete old WAL files  - data is now durable in SSTable
	if err := lsm.wal.deleteFiles(paths); err != nil {
		return fmt.Errorf("recovery: %w", err)
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
				lsm.mergeSSTablesAcrossAllLevels()
			}()
		case <-lsm.done:
			lsm.compactTicker.Stop()
			return

		}

	}
}

func (lsm *LSM) mergeSSTablesAcrossAllLevels() {
	caps := [numLevels]int{capL0, capL1, capL2, capL3}

	for level := 0; level < numLevels-1; level++ {
		if len(lsm.levels[level]) <= caps[level] {
			continue
		}

		targetLevel := level + 1

		for _, sourceTable := range lsm.levels[level] {
			overlapping := findOverlapping(sourceTable, lsm.levels[targetLevel])
			sources := append([]SSTable{sourceTable}, overlapping...)

			newTables, err := lsm.mergeAndPersist(sources, targetLevel)
			if err != nil {
				fmt.Printf("compaction L%d→L%d failed: %v\n", level, targetLevel, err)
				return
			}

			// delete old source SSTables from disk
			for _, sst := range sources {
				os.RemoveAll(sst.PathRef)
			}

			// rebuild source level: remove the source table
			lsm.levels[level] = removeSSTable(lsm.levels[level], sourceTable)

			// rebuild target level: remove overlapping, add new
			for _, sst := range overlapping {
				lsm.levels[targetLevel] = removeSSTable(lsm.levels[targetLevel], sst)
			}
			lsm.levels[targetLevel] = append(newTables, lsm.levels[targetLevel]...)
		}
	}
}

// removeSSTable removes an SSTable from a slice by PathRef.
func removeSSTable(tables []SSTable, target SSTable) []SSTable {
	var result []SSTable
	for _, t := range tables {
		if t.PathRef != target.PathRef {
			result = append(result, t)
		}
	}
	return result
}

// findOverlapping returns all SSTables in the target level whose key range overlaps with the source SSTable.
func findOverlapping(source SSTable, level []SSTable) []SSTable {
	var result []SSTable
	for _, table := range level {
		if bytes.Compare(source.MaxKey, table.MinKey) >= 0 && bytes.Compare(source.MinKey, table.MaxKey) <= 0 {
			result = append(result, table)
		}
	}
	return result
}

// readAllEntries reads all KV entries from an SSTable file, skipping the bloom filter header.
func readAllEntries(sstableDir string) ([]KVEntry, error) {
	dataPath := filepath.Join(sstableDir, ".data")
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("read sstable: open %s: %w", dataPath, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

	var entries []KVEntry
	for {
		entry, err := ReadKVEntry(r)
		if err != nil {
			return nil, fmt.Errorf("read sstable: %s: %w", dataPath, err)
		}
		if entry == nil {
			break
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}

// mergeEntries merge-sorts entries from multiple SSTables.
// Entries are already sorted within each SSTable. For duplicate keys, the entry
// from the earlier source (lower index) wins  - callers must pass sources with
// newest SSTable first. At the final level, tombstones are dropped.
func mergeEntries(sources [][]KVEntry, isFinalLevel bool) []KVEntry {
	// flatten all entries
	var all []KVEntry
	for _, entries := range sources {
		all = append(all, entries...)
	}

	// sort by key
	sort.Slice(all, func(i, j int) bool {
		return bytes.Compare(all[i].Key, all[j].Key) < 0
	})

	// deduplicate: for runs of the same key, keep the first occurrence.
	// since the source SSTable (newest) is listed first in the input,
	// and sort is stable for equal keys in insertion order, the first
	// occurrence is the newest version.
	var merged []KVEntry
	for i, entry := range all {
		if i > 0 && bytes.Equal(entry.Key, all[i-1].Key) {
			continue // duplicate key, skip older version
		}
		// at the final level, drop tombstones  - no deeper level to propagate to
		if isFinalLevel && entry.Tombstone == 1 {
			continue
		}
		merged = append(merged, entry)
	}

	return merged
}

// mergeAndPersist merges the given SSTables via merge-sort, writes new non-overlapping SSTables
// to the target level directory, and returns the new SSTables.
// At the final level (L3), tombstones are dropped.
func (lsm *LSM) mergeAndPersist(sources []SSTable, targetLevel int) ([]SSTable, error) {
	// 1. read all entries from source SSTables (newest first)
	allSources := make([][]KVEntry, len(sources))
	for i, sst := range sources {
		entries, err := readAllEntries(sst.PathRef)
		if err != nil {
			return nil, err
		}
		allSources[i] = entries
	}

	// 2. merge-sort and deduplicate
	isFinalLevel := targetLevel == 3
	merged := mergeEntries(allSources, isFinalLevel)

	if len(merged) == 0 {
		return nil, nil
	}

	// 3. build a skiplist from merged entries and write as SSTable
	// reuses writeSkipListToSSTable which handles bloom filter, fsync, manifest
	sl := NewSkipList()
	for _, entry := range merged {
		if entry.Tombstone == 1 {
			sl.InsertWithTombstone(entry.Key)
		} else {
			sl.Insert(entry.Key, entry.Val)
		}
	}

	if err := lsm.writeSkipListToSSTable(sl, targetLevel); err != nil {
		return nil, fmt.Errorf("compaction: write merged sstable: %w", err)
	}

	// the new SSTable was prepended to lsm.levels[targetLevel] by writeSkipListToSSTable
	newSST := lsm.levels[targetLevel][0]

	return []SSTable{newSST}, nil
}
