package storage

import (
	"math/big"
)

const base62Charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// Base62Encode encodes a byte slice into a base62 string
func Base62Encode(data []byte) string {
	if len(data) == 0 {
		return "0"
	}

	n := new(big.Int).SetBytes(data)
	if n.Sign() == 0 {
		return "0"
	}

	base := big.NewInt(62)
	mod := new(big.Int)
	var result []byte

	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		result = append(result, base62Charset[mod.Int64()])
	}

	// Reverse
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return string(result)
}

// Base62Decode decodes a base62 string back to a byte slice
func Base62Decode(s string) []byte {
	if s == "0" {
		return []byte{0}
	}

	n := new(big.Int)
	base := big.NewInt(62)

	for _, c := range s {
		idx := int64(charIndex(byte(c)))
		n.Mul(n, base)
		n.Add(n, big.NewInt(idx))
	}

	return n.Bytes()
}

func charIndex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'A' && c <= 'Z':
		return int(c-'A') + 10
	case c >= 'a' && c <= 'z':
		return int(c-'a') + 36
	}
	return 0
}
