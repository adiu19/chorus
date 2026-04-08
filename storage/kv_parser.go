package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// ReadKVEntry reads one KV entry from a buffered reader.
// Format: [1B tombstone][1B key_len][key][2B val_len][value]
// Returns (entry, nil) on success, (nil, nil) on clean EOF.
func ReadKVEntry(r *bufio.Reader) (*KVEntry, error) {
	tomb, err := r.ReadByte()
	if err != nil {
		return nil, nil // EOF — no more entries
	}

	keyLen, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read entry: key len: %w", err)
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, fmt.Errorf("read entry: key: %w", err)
	}

	valLenBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, valLenBuf); err != nil {
		return nil, fmt.Errorf("read entry: val len: %w", err)
	}
	valLen := binary.BigEndian.Uint16(valLenBuf)

	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		return nil, fmt.Errorf("read entry: value: %w", err)
	}

	return &KVEntry{
		Tombstone: tomb,
		KeySize:   keyLen,
		Key:       key,
		ValSize:   valLen,
		Val:       val,
	}, nil
}

// WriteKVEntry writes one KV entry to a buffered writer.
// Format: [1B tombstone][1B key_len][key][2B val_len][value]
func WriteKVEntry(w *bufio.Writer, key []byte, value []byte, tombstone byte) (int, error) {
	bytesWritten := 0

	if err := w.WriteByte(tombstone); err != nil {
		return 0, fmt.Errorf("write entry: tombstone: %w", err)
	}
	bytesWritten++

	if err := w.WriteByte(uint8(len(key))); err != nil {
		return 0, fmt.Errorf("write entry: key len: %w", err)
	}
	bytesWritten++

	if _, err := w.Write(key); err != nil {
		return 0, fmt.Errorf("write entry: key: %w", err)
	}
	bytesWritten += len(key)

	valLenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(valLenBuf, uint16(len(value)))
	if _, err := w.Write(valLenBuf); err != nil {
		return 0, fmt.Errorf("write entry: val len: %w", err)
	}
	bytesWritten += 2

	if _, err := w.Write(value); err != nil {
		return 0, fmt.Errorf("write entry: value: %w", err)
	}
	bytesWritten += len(value)

	return bytesWritten, nil
}
