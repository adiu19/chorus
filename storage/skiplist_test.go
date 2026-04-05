package storage

import (
	"fmt"
	"testing"
)

func TestInsertAndGet(t *testing.T) {
	sl := NewSkipList()

	sl.Insert([]byte("banana"), []byte("yellow"))
	sl.Insert([]byte("apple"), []byte("red"))
	sl.Insert([]byte("cherry"), []byte("dark red"))

	val := sl.Get([]byte("apple"))
	if string(val) != "red" {
		t.Fatalf("expected 'red', got '%s'", val)
	}

	val = sl.Get([]byte("banana"))
	if string(val) != "yellow" {
		t.Fatalf("expected 'yellow', got '%s'", val)
	}

	val = sl.Get([]byte("cherry"))
	if string(val) != "dark red" {
		t.Fatalf("expected 'dark red', got '%s'", val)
	}
}

func TestGetMissing(t *testing.T) {
	sl := NewSkipList()

	sl.Insert([]byte("apple"), []byte("red"))

	val := sl.Get([]byte("banana"))
	if val != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestInsertOverwrite(t *testing.T) {
	sl := NewSkipList()

	sl.Insert([]byte("apple"), []byte("red"))
	sl.Insert([]byte("apple"), []byte("green"))

	val := sl.Get([]byte("apple"))
	if string(val) != "green" {
		t.Fatalf("expected 'green' after overwrite, got '%s'", val)
	}
}

func TestDelete(t *testing.T) {
	sl := NewSkipList()

	sl.Insert([]byte("apple"), []byte("red"))
	sl.Insert([]byte("banana"), []byte("yellow"))
	sl.Insert([]byte("cherry"), []byte("dark red"))

	err := sl.Delete([]byte("banana"))
	if err != nil {
		t.Fatalf("expected delete to succeed: %v", err)
	}

	val := sl.Get([]byte("banana"))
	if val != nil {
		t.Fatal("expected banana to be gone after delete")
	}

	// Other keys still accessible
	val = sl.Get([]byte("apple"))
	if string(val) != "red" {
		t.Fatalf("expected 'red', got '%s'", val)
	}

	val = sl.Get([]byte("cherry"))
	if string(val) != "dark red" {
		t.Fatalf("expected 'dark red', got '%s'", val)
	}
}

func TestDeleteMissing(t *testing.T) {
	sl := NewSkipList()

	sl.Insert([]byte("apple"), []byte("red"))

	err := sl.Delete([]byte("banana"))
	if err == nil {
		t.Fatal("expected error deleting missing key")
	}
}

func TestSortedIteration(t *testing.T) {
	sl := NewSkipList()

	keys := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for _, k := range keys {
		sl.Insert([]byte(k), []byte("val"))
	}

	// Walk level 0 — should be sorted
	expected := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	node := sl.Head.Forward[0]
	for i, exp := range expected {
		if node == nil {
			t.Fatalf("ran out of nodes at index %d", i)
		}
		if string(node.Key) != exp {
			t.Fatalf("index %d: expected '%s', got '%s'", i, exp, string(node.Key))
		}
		node = node.Forward[0]
	}
	if node != nil {
		t.Fatal("expected end of list, but more nodes remain")
	}
}

func TestManyInserts(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%05d", i)
		val := fmt.Sprintf("val:%05d", i)
		err := sl.Insert([]byte(key), []byte(val))
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// Verify all retrievable
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%05d", i)
		expected := fmt.Sprintf("val:%05d", i)
		val := sl.Get([]byte(key))
		if val == nil {
			t.Fatalf("get %d returned nil", i)
		}
		if string(val) != expected {
			t.Fatalf("key %d: expected '%s', got '%s'", i, expected, string(val))
		}
	}

	// Verify sorted order at level 0
	node := sl.Head.Forward[0]
	var prev []byte
	for node != nil {
		if prev != nil && string(node.Key) <= string(prev) {
			t.Fatalf("sort order broken: '%s' after '%s'", node.Key, prev)
		}
		prev = node.Key
		node = node.Forward[0]
	}
}
