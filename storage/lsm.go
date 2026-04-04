package storage

// LSM is a wrapper over our data store
type LSM struct {
	MemTable *SkipList
	Manifest *LSMManifest
}

// LSMManifest captures the metadata required to load the data references
type LSMManifest struct {
	RootPath         string
	OrderedTableRefs []SSTable
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
