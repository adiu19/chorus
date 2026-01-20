package cluster

import (
	"sync"
)

const StaleThreshold = 3 // mark dead after this many cycles without heartbeat change

// Peer represents a known node in the cluster.
type Peer struct {
	Addr       string // host:port
	Heartbeat  int64  // the node's heartbeat counter
	StaleCount int    // cycles since heartbeat changed
	IsAlive    bool
}

// PeerList manages the set of known peers in the cluster.
type PeerList struct {
	mu    sync.RWMutex
	peers map[string]*Peer // keyed by address
}

// NewPeerList creates a PeerList initialized with the given seed addresses.
func NewPeerList(seeds []string) *PeerList {
	pl := &PeerList{
		peers: make(map[string]*Peer),
	}

	for _, addr := range seeds {
		pl.peers[addr] = &Peer{
			Addr:       addr,
			Heartbeat:  0,
			StaleCount: 0,
			IsAlive:    true,
		}
	}

	return pl
}

// GetHeartbeats returns a map of peer address -> heartbeat counter.
func (pl *PeerList) GetHeartbeats() map[string]int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make(map[string]int64, len(pl.peers))
	for addr, p := range pl.peers {
		result[addr] = p.Heartbeat
	}
	return result
}

// MergeHeartbeats merges incoming heartbeats with local state.
// For each peer:
//   - If incoming heartbeat > local: update and reset stale count
//   - If incoming heartbeat <= local: increment stale count
//   - If stale count > threshold: mark dead
//
// Returns the merged heartbeats (max of local and incoming).
func (pl *PeerList) MergeHeartbeats(incoming map[string]int64) map[string]int64 {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// First, add any new peers we don't know about
	for addr, hb := range incoming {
		if _, exists := pl.peers[addr]; !exists {
			pl.peers[addr] = &Peer{
				Addr:       addr,
				Heartbeat:  hb,
				StaleCount: 0,
				IsAlive:    true,
			}
		}
	}

	// Build merged result and update local state
	result := make(map[string]int64, len(pl.peers))

	for addr, p := range pl.peers {
		incomingHb, inIncoming := incoming[addr]

		if inIncoming && incomingHb > p.Heartbeat {
			// Incoming has fresher heartbeat
			p.Heartbeat = incomingHb
			p.StaleCount = 0
			p.IsAlive = true
		}
		// Note: we don't increment stale count here - that happens in ProcessResponse

		result[addr] = p.Heartbeat
	}

	return result
}

// ProcessResponse processes a ping response, updating stale counts.
// Call this after receiving a successful ping response.
func (pl *PeerList) ProcessResponse(responseHeartbeats map[string]int64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	for addr, p := range pl.peers {
		incomingHb, inResponse := responseHeartbeats[addr]

		if inResponse && incomingHb > p.Heartbeat {
			// Fresher heartbeat received
			p.Heartbeat = incomingHb
			p.StaleCount = 0
			p.IsAlive = true
		} else {
			// No update for this peer
			p.StaleCount++
			if p.StaleCount > StaleThreshold {
				p.IsAlive = false
			}
		}
	}
}

// IncrementStaleCount increments the stale count for a peer (e.g., on ping failure).
func (pl *PeerList) IncrementStaleCount(addr string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if p, exists := pl.peers[addr]; exists {
		p.StaleCount++
		if p.StaleCount > StaleThreshold {
			p.IsAlive = false
		}
	}
}

// GetAll returns a copy of all peers.
func (pl *PeerList) GetAll() []Peer {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Peer, 0, len(pl.peers))
	for _, p := range pl.peers {
		result = append(result, *p)
	}
	return result
}

// GetAlive returns a copy of all alive peers.
func (pl *PeerList) GetAlive() []Peer {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Peer, 0)
	for _, p := range pl.peers {
		if p.IsAlive {
			result = append(result, *p)
		}
	}
	return result
}

// GetAddresses returns a list of all peer addresses.
func (pl *PeerList) GetAddresses() []string {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]string, 0, len(pl.peers))
	for addr := range pl.peers {
		result = append(result, addr)
	}
	return result
}
