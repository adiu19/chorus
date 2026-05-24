package hashes

import (
	"hash/crc32"
)

// HashViaCRC32 hashes a byte stream via CRC32
func HashViaCRC32(in []byte) int {
	return int(crc32.ChecksumIEEE(in))
}
