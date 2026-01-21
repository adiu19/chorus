package cluster

import (
	"sync"
)

const StaleThreshold = 3 // mark dead after this many cycles without heartbeat change

// PeerInfo is the data exchanged in gossip (matches proto PeerInfo).
type PeerInfo struct {
	ID        string
	Addr      string
	Heartbeat int64
}

// Peer represents a known node in the cluster.
type Peer struct {
	ID         string // node ID (e.g., "node1")
	Addr       string // address (e.g., "localhost:8010")
	Heartbeat  int64  // the node's heartbeat counter
	StaleCount int    // cycles since heartbeat changed
	IsAlive    bool
}

// PeerList manages the set of known peers in the cluster.
type PeerList struct {
	mu       sync.RWMutex
	peers    map[string]*Peer // keyed by node ID
	selfID   string           // our own node ID (to filter out self)
	selfAddr string           // our own address (to filter out self)
}

// NewPeerList creates an empty PeerList.
// Seeds are now added via MergePeers after learning IDs from first contact.
func NewPeerList(selfID, selfAddr string) *PeerList {
	return &PeerList{
		peers:    make(map[string]*Peer),
		selfID:   selfID,
		selfAddr: selfAddr,
	}
}

// AddSeedAddr adds a seed by address only (ID unknown until first contact).
// We use the address as a temporary ID until we learn the real ID.
func (pl *PeerList) AddSeedAddr(addr string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Use address as temporary ID - will be updated when we learn real ID
	if _, exists := pl.peers[addr]; !exists {
		pl.peers[addr] = &Peer{
			ID:         addr, // temporary, will be replaced
			Addr:       addr,
			Heartbeat:  0,
			StaleCount: 0,
			IsAlive:    true,
		}
	}
}

// GetPeerInfos returns all peers as PeerInfo slice (for gossip).
// Excludes unresolved seed entries (where ID == Addr).
func (pl *PeerList) GetPeerInfos() []PeerInfo {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]PeerInfo, 0, len(pl.peers))
	for _, p := range pl.peers {
		// Skip unresolved seed entries (ID == Addr means we haven't learned real ID yet)
		if p.ID == p.Addr {
			continue
		}
		result = append(result, PeerInfo{
			ID:        p.ID,
			Addr:      p.Addr,
			Heartbeat: p.Heartbeat,
		})
	}
	return result
}

// MergePeers merges incoming peer infos with local state.
// Returns merged peer infos and whether any new peer was discovered.
func (pl *PeerList) MergePeers(incoming []PeerInfo) ([]PeerInfo, bool) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	newPeerDiscovered := false

	// Add/update peers from incoming
	for _, info := range incoming {
		if info.ID == "" {
			continue // skip invalid entries
		}
		// Skip self
		if info.ID == pl.selfID || info.Addr == pl.selfAddr {
			continue
		}

		p, exists := pl.peers[info.ID]
		if !exists {
			// New peer discovered - remove any stale address-keyed entry
			// (seeds are initially keyed by address until we learn real ID)
			if info.Addr != "" {
				if _, addrExists := pl.peers[info.Addr]; addrExists {
					delete(pl.peers, info.Addr)
				}
			}

			pl.peers[info.ID] = &Peer{
				ID:         info.ID,
				Addr:       info.Addr,
				Heartbeat:  info.Heartbeat,
				StaleCount: 0,
				IsAlive:    true,
			}
			newPeerDiscovered = true
		} else {
			// Update address if we have it
			if info.Addr != "" && p.Addr != info.Addr {
				p.Addr = info.Addr
			}
			// Update heartbeat if incoming is fresher
			if info.Heartbeat > p.Heartbeat {
				p.Heartbeat = info.Heartbeat
				p.StaleCount = 0
				p.IsAlive = true
			}
		}
	}

	// Build merged result (exclude unresolved seeds)
	result := make([]PeerInfo, 0, len(pl.peers))
	for _, p := range pl.peers {
		if p.ID == p.Addr {
			continue
		}
		result = append(result, PeerInfo{
			ID:        p.ID,
			Addr:      p.Addr,
			Heartbeat: p.Heartbeat,
		})
	}

	return result, newPeerDiscovered
}

// ProcessResponse processes a ping response, updating stale counts.
// Returns true if any peer was marked dead (ring rebalance needed).
func (pl *PeerList) ProcessResponse(responsePeers []PeerInfo) bool {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Build a map for quick lookup
	responseMap := make(map[string]PeerInfo, len(responsePeers))
	for _, info := range responsePeers {
		responseMap[info.ID] = info
	}

	peerMarkedDead := false

	for id, p := range pl.peers {
		info, inResponse := responseMap[id]

		if inResponse && info.Heartbeat > p.Heartbeat {
			// Fresher heartbeat received
			p.Heartbeat = info.Heartbeat
			p.StaleCount = 0
			p.IsAlive = true
		} else {
			// No update for this peer
			p.StaleCount++
			if p.StaleCount > StaleThreshold {
				if p.IsAlive {
					peerMarkedDead = true
				}
				p.IsAlive = false
			}
		}
	}

	return peerMarkedDead
}

// IncrementStaleCount increments the stale count for a peer by ID.
// Returns true if the peer was marked dead.
func (pl *PeerList) IncrementStaleCount(id string) bool {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if p, exists := pl.peers[id]; exists {
		p.StaleCount++
		if p.StaleCount > StaleThreshold {
			if p.IsAlive {
				p.IsAlive = false
				return true
			}
		}
	}
	return false
}

// GetAddress returns the address for a node ID.
func (pl *PeerList) GetAddress(id string) string {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	if p, exists := pl.peers[id]; exists {
		return p.Addr
	}
	return ""
}

// GetAll returns a copy of all peers.
// Excludes unresolved seed entries.
func (pl *PeerList) GetAll() []Peer {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Peer, 0, len(pl.peers))
	for _, p := range pl.peers {
		if p.ID != p.Addr {
			result = append(result, *p)
		}
	}
	return result
}

// GetAlive returns a copy of all alive peers.
// Excludes unresolved seed entries.
func (pl *PeerList) GetAlive() []Peer {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]Peer, 0)
	for _, p := range pl.peers {
		if p.IsAlive && p.ID != p.Addr {
			result = append(result, *p)
		}
	}
	return result
}

// GetAliveForGossip returns all alive peers INCLUDING unresolved seeds.
// Use this for selecting gossip targets (we need to contact seeds to learn their IDs).
func (pl *PeerList) GetAliveForGossip() []Peer {
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

// GetAliveIDs returns the IDs of all alive peers (for ring rebalancing).
// Excludes unresolved seed entries.
func (pl *PeerList) GetAliveIDs() []string {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]string, 0)
	for _, p := range pl.peers {
		if p.IsAlive && p.ID != p.Addr {
			result = append(result, p.ID)
		}
	}
	return result
}

// GetIDs returns all peer IDs.
func (pl *PeerList) GetIDs() []string {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	result := make([]string, 0, len(pl.peers))
	for id := range pl.peers {
		result = append(result, id)
	}
	return result
}
