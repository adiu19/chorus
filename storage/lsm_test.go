package storage

import (
	"fmt"
	"os"
	"testing"
)

func setupLSM(t *testing.T) *LSM {
	t.Helper()
	dir, err := os.MkdirTemp("", "lsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	lsm, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("failed to create LSM: %v", err)
	}
	return lsm
}

func TestLSMInsertAndGetFromMemtable(t *testing.T) {
	lsm := setupLSM(t)

	lsm.Insert([]byte("apple"), []byte("red"))
	lsm.Insert([]byte("banana"), []byte("yellow"))

	val, err := lsm.Get([]byte("apple"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != "red" {
		t.Fatalf("expected 'red', got '%s'", val)
	}

	val, err = lsm.Get([]byte("banana"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != "yellow" {
		t.Fatalf("expected 'yellow', got '%s'", val)
	}
}

func TestLSMGetMissing(t *testing.T) {
	lsm := setupLSM(t)

	val, err := lsm.Get([]byte("ghost"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Fatalf("expected nil for missing key, got '%s'", val)
	}
}

func TestLSMFlushAndReadFromSSTable(t *testing.T) {
	lsm := setupLSM(t)

	lsm.Insert([]byte("apple"), []byte("red"))
	lsm.Insert([]byte("banana"), []byte("yellow"))
	lsm.Insert([]byte("cherry"), []byte("dark red"))

	err := lsm.Flush()
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Memtable is now empty, data is on disk
	val, err := lsm.Get([]byte("apple"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != "red" {
		t.Fatalf("expected 'red', got '%s'", val)
	}

	val, err = lsm.Get([]byte("cherry"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != "dark red" {
		t.Fatalf("expected 'dark red', got '%s'", val)
	}
}

func TestLSMGetMissingAfterFlush(t *testing.T) {
	lsm := setupLSM(t)

	lsm.Insert([]byte("apple"), []byte("red"))
	lsm.Flush()

	val, err := lsm.Get([]byte("ghost"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Fatalf("expected nil for missing key, got '%s'", val)
	}
}

func TestLSMMemtableOverridesSSTable(t *testing.T) {
	lsm := setupLSM(t)

	// Write and flush
	lsm.Insert([]byte("apple"), []byte("red"))
	lsm.Flush()

	// Write a new value for the same key to memtable
	lsm.Insert([]byte("apple"), []byte("green"))

	// Should get the memtable value, not the SSTable value
	val, err := lsm.Get([]byte("apple"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != "green" {
		t.Fatalf("expected 'green', got '%s'", val)
	}
}

func TestLSMMultipleFlushes(t *testing.T) {
	lsm := setupLSM(t)

	// First batch
	lsm.Insert([]byte("apple"), []byte("red"))
	lsm.Insert([]byte("banana"), []byte("yellow"))
	lsm.Flush()

	// Second batch
	lsm.Insert([]byte("cherry"), []byte("dark red"))
	lsm.Insert([]byte("date"), []byte("brown"))
	lsm.Flush()

	// All keys should be readable across both SSTables
	cases := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "dark red",
		"date":   "brown",
	}
	for k, expected := range cases {
		val, err := lsm.Get([]byte(k))
		if err != nil {
			t.Fatalf("key %s: unexpected error: %v", k, err)
		}
		if string(val) != expected {
			t.Fatalf("key %s: expected '%s', got '%s'", k, expected, string(val))
		}
	}
}

func TestLSMNewerSSTableWins(t *testing.T) {
	lsm := setupLSM(t)

	// Flush with old value
	lsm.Insert([]byte("apple"), []byte("red"))
	lsm.Flush()

	// Flush with new value
	lsm.Insert([]byte("apple"), []byte("green"))
	lsm.Flush()

	// Should get the newer value
	val, err := lsm.Get([]byte("apple"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != "green" {
		t.Fatalf("expected 'green', got '%s'", val)
	}
}

func TestLSMManyKeysFlushAndRead(t *testing.T) {
	lsm := setupLSM(t)

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key:%05d", i)
		val := fmt.Sprintf("val:%05d", i)
		lsm.Insert([]byte(key), []byte(val))
	}
	lsm.Flush()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key:%05d", i)
		expected := fmt.Sprintf("val:%05d", i)
		val, err := lsm.Get([]byte(key))
		if err != nil {
			t.Fatalf("key %d: unexpected error: %v", i, err)
		}
		if string(val) != expected {
			t.Fatalf("key %d: expected '%s', got '%s'", i, expected, string(val))
		}
	}
}
