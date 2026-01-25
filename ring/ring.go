package ring

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
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
	mu          sync.RWMutex
	ring        []virtualNode // sorted by position
	Fingerprint uint32        // hash of sorted node IDs - same fingerprint = same membership
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

	// Sort nodes for deterministic fingerprint
	sorted := make([]string, len(nodes))
	copy(sorted, nodes)
	sort.Strings(sorted)

	// Compute fingerprint from sorted membership
	r.Fingerprint = crc32.ChecksumIEEE([]byte(strings.Join(sorted, ",")))

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

// GetNodes returns the first n distinct physical nodes clockwise from the key's position.
// Used for replication - returns [primary, replica2, replica3, ...].
func (r *Ring) GetNodes(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return nil, ErrEmptyRing
	}

	position := hash(key)

	// binary search for first node with position >= hash
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].position >= position
	})

	// wrap around to first node if past the end
	if idx >= len(r.ring) {
		idx = 0
	}

	// Walk clockwise collecting distinct physical nodes
	nodes := make([]string, 0, n)
	seen := make(map[string]bool)

	for len(nodes) < n {
		node := r.ring[idx].node
		if !seen[node] {
			seen[node] = true
			nodes = append(nodes, node)
		}
		idx = (idx + 1) % len(r.ring)

		// If we've wrapped around completely, we've seen all nodes
		if len(seen) >= len(r.ring)/NumVirtualNodes {
			break
		}
	}

	return nodes, nil
}

func hash(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}
