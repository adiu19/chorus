package cluster

import (
	"sync"
	"time"
)

// Peer represents a known node in the cluster.
type Peer struct {
	Addr     string // host:port
	LastSeen int64  // unix epoch seconds
	IsAlive  bool
}

// PeerList manages the set of known peers in the cluster.
type PeerList struct {
	mu    sync.RWMutex
	peers map[string]*Peer // keyed by address
}

// NewPeerList creates a PeerList initialized with the given seed addresses.
// Seeds are marked as alive with LastSeen set to now.
func NewPeerList(seeds []string) *PeerList {
	pl := &PeerList{
		peers: make(map[string]*Peer),
	}

	now := time.Now().Unix()
	for _, addr := range seeds {
		pl.peers[addr] = &Peer{
			Addr:     addr,
			LastSeen: now,
			IsAlive:  true,
		}
	}

	return pl
}

// Add adds a new peer if it doesn't exist.
// Does not modify existing peers - use MarkAlive for that.
func (pl *PeerList) Add(addr string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if _, exists := pl.peers[addr]; !exists {
		pl.peers[addr] = &Peer{
			Addr:     addr,
			LastSeen: time.Now().Unix(),
			IsAlive:  true,
		}
	}
}

// MarkAlive updates LastSeen and sets IsAlive to true for the given peer.
func (pl *PeerList) MarkAlive(addr string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if p, exists := pl.peers[addr]; exists {
		p.LastSeen = time.Now().Unix()
		p.IsAlive = true
	}
}

// MarkDead sets IsAlive to false for the given peer.
func (pl *PeerList) MarkDead(addr string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if p, exists := pl.peers[addr]; exists {
		p.IsAlive = false
	}
}

// GetAll returns a copy of all peers.
func (pl *PeerList) GetAll() []Peer {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Peer, 0, len(pl.peers))
	for _, p := range pl.peers {
		result = append(result, *p) // copy
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

// GetAddresses returns a list of all peer addresses (for use in Ping RPC).
func (pl *PeerList) GetAddresses() []string {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]string, 0, len(pl.peers))
	for addr := range pl.peers {
		result = append(result, addr)
	}
	return result
}
