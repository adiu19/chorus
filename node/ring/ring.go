package ring

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// NumVirtualNodes is the number of virtual nodes we want per node on the ring
const NumVirtualNodes = 150

// ErrEmptyRing is an error if the ring has no nodes
var ErrEmptyRing = errors.New("ring is empty")

type virtualNode struct {
	position int
	node     string
}

// Ring encapsulates info on the hash ring we're maintaining
type Ring struct {
	mu      sync.RWMutex
	ring    []virtualNode // sorted by position
	Version int16
}

// New inits a new ring
func New() *Ring {
	return &Ring{
		ring: []virtualNode{},
	}
}

// Rebalance creates a new ring arrangement based on the given nodes and a constant number of virtual nodes.
func (r *Ring) Rebalance(nodes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ring = r.ring[:0] // clear

	for _, node := range nodes {
		for i := 0; i < NumVirtualNodes; i++ {
			position := hash(fmt.Sprintf("%s-%d", node, i))
			r.ring = append(r.ring, virtualNode{position: position, node: node})
		}
	}

	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i].position < r.ring[j].position
	})
}

// GetNode returns the node id that this request should map to.
func (r *Ring) GetNode(requestID string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return "", ErrEmptyRing
	}

	position := hash(requestID)

	// binary search for first node with position >= hash
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].position >= position
	})

	// wrap around to first node if past the end
	if idx >= len(r.ring) {
		idx = 0
	}

	return r.ring[idx].node, nil
}

func hash(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}
