package storage

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
)

// MaxLevels in our skip list
const MaxLevels = 20

// Node defines a single entry in our list
type Node struct {
	Key       []byte
	Value     []byte
	Forward   []*Node
	Tombstone byte
}

// SkipList encapsulates a skip list
type SkipList struct {
	Head        *Node
	mu          sync.RWMutex
	SizeInBytes int
}

// NewSkipList inits a new skip lsit
func NewSkipList() *SkipList {
	head := &Node{
		Forward: make([]*Node, MaxLevels),
	}
	return &SkipList{
		Head: head,
	}
}

// InsertWithTombstone adds a new item into our skiplist with the tombstone marker set
func (sl *SkipList) InsertWithTombstone(key []byte) error {
	return sl.insert(key, nil, 1)
}

// Insert adds a new item into our skiplist
func (sl *SkipList) Insert(key []byte, value []byte) error {
	return sl.insert(key, value, 0)
}

func (sl *SkipList) insert(key []byte, value []byte, tombstone byte) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	var predecessors [MaxLevels]*Node
	sl.SizeInBytes = sl.SizeInBytes + 1 + len(key) + len(value)
	currNode := sl.Head
	currLevelID := MaxLevels - 1
	for currLevelID >= 0 {
		for currNode.Forward[currLevelID] != nil {
			cmp := bytes.Compare(currNode.Forward[currLevelID].Key, key)
			if cmp < 0 {
				currNode = currNode.Forward[currLevelID]
			} else if cmp == 0 {
				sl.SizeInBytes = sl.SizeInBytes - len(currNode.Forward[currLevelID].Value) - len(key) - 1
				// Overwrite existing key
				currNode.Forward[currLevelID].Value = value
				currNode.Forward[currLevelID].Tombstone = tombstone

				return nil
			} else {
				break
			}
		}
		predecessors[currLevelID] = currNode
		currLevelID--
	}

	numLevels := 1 // by default, the node will always be at level = 0
	for level := 1; level < MaxLevels; level++ {
		if rand.Float64() <= 0.5 {
			numLevels++
		} else {
			// the node is not going to be on any more levels
			break
		}
	}

	newNode := &Node{
		Key:       key,
		Value:     value,
		Forward:   make([]*Node, numLevels),
		Tombstone: tombstone,
	}

	// insert node at level 0
	newNode.insertAfterPredecessor(predecessors[0], 0)
	for level := 1; level < numLevels; level++ {
		newNode.insertAfterPredecessor(predecessors[level], level)
	}

	return nil

}

func (n *Node) insertAfterPredecessor(pred *Node, level int) {
	next := pred.Forward[level]
	n.Forward[level] = next
	pred.Forward[level] = n
}

// Get looks up a key and returns its value
func (sl *SkipList) Get(key []byte) []byte {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	currNode := sl.Head
	for level := MaxLevels - 1; level >= 0; level-- {
		for currNode.Forward[level] != nil {
			cmp := bytes.Compare(currNode.Forward[level].Key, key)
			if cmp < 0 {
				currNode = currNode.Forward[level]
			} else if cmp == 0 {
				if currNode.Forward[level].Tombstone == 1 {
					return nil
				}
				return currNode.Forward[level].Value
			} else {
				break
			}
		}
	}
	return nil
}

// Delete removes a key from the skiplist
func (sl *SkipList) Delete(key []byte) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	var predecessors [MaxLevels]*Node

	currNode := sl.Head
	found := false
	for level := MaxLevels - 1; level >= 0; level-- {
		for currNode.Forward[level] != nil {
			cmp := bytes.Compare(currNode.Forward[level].Key, key)
			if cmp < 0 {
				currNode = currNode.Forward[level]
			} else if cmp == 0 {
				found = true
				predecessors[level] = currNode
				break
			} else {
				break
			}
		}
	}

	if !found {
		return errors.New("key not found")
	}

	// Unlink the node at each level it appears on
	for level := 0; level < MaxLevels; level++ {
		if predecessors[level] == nil {
			continue
		}
		target := predecessors[level].Forward[level]
		predecessors[level].Forward[level] = target.Forward[level]
	}

	return nil
}
