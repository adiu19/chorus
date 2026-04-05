package storage

import (
	"hash"
	"hash/fnv"
)

const bloomFilterSize = 8 * 1024            // 8KB bitmap
const bloomFilterBits = bloomFilterSize * 8 // 65536 bits

// BloomFilter represents our bloom filter structure
type BloomFilter struct {
	BitMap   []byte
	Modulo   uint32
	HashFunc hash.Hash64
}

// NewBloomFilter inits a new bloom filter
func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		BitMap:   make([]byte, bloomFilterSize),
		Modulo:   bloomFilterBits,
		HashFunc: fnv.New64a(),
	}
}

func (bf *BloomFilter) hash(key []byte) (uint32, uint32) {
	bf.HashFunc.Reset()
	bf.HashFunc.Write(key)
	sum := bf.HashFunc.Sum64()
	h1 := uint32(sum >> 32) // upper 32 bits
	h2 := uint32(sum)       // lower 32 bits
	return h1, h2
}

// setBit sets the bit at position p in the bitmap
func (bf *BloomFilter) setBit(p uint32) {
	bf.BitMap[p/8] |= 1 << (p % 8)
}

// getBit checks the bit at position p in the bitmap
func (bf *BloomFilter) getBit(p uint32) bool {
	return bf.BitMap[p/8]&(1<<(p%8)) != 0
}

// Exists does a probabilistic check to see if the key exists in our store or not.
// if it says no, we know for sure it doesn't exist
// if it says yes, it might or might not exist
func (bf *BloomFilter) Exists(key []byte) bool {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < 5; i++ {
		pos := (h1 + i*h2) % bf.Modulo
		if !bf.getBit(pos) {
			return false
		}
	}
	return true
}

// Capture adds a key to our bitmap
func (bf *BloomFilter) Capture(key []byte) {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < 5; i++ {
		pos := (h1 + i*h2) % bf.Modulo
		bf.setBit(pos)
	}
}
