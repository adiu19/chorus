package node

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/chorus/cluster"
	pb "github.com/chorus/proto"
	"github.com/chorus/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	ID   string
	Port string
	Addr string // "localhost:port" - this node's address

	peers *cluster.PeerList
	ring  *ring.Ring

	mu        sync.RWMutex
	heartbeat int64 // this node's heartbeat counter, only we increment this

	dataMu sync.RWMutex
	data   map[string][]byte // key-value storage
}

func NewNode(id, port string, seeds []string) *Node {
	addr := "localhost:" + port

	node := &Node{
		ID:        id,
		Port:      port,
		Addr:      addr,
		peers:     cluster.NewPeerList(id, addr),
		ring:      ring.New(),
		heartbeat: 0,
		data:      make(map[string][]byte),
	}

	// Add seeds (we don't know their IDs yet, use address as temp ID)
	// Skip our own address
	for _, seedAddr := range seeds {
		if seedAddr != addr {
			node.peers.AddSeedAddr(seedAddr)
		}
	}

	// Initial ring with just ourselves
	node.ring.Rebalance([]string{id})

	log.Printf("[%s] Node starting on :%s", node.ID, node.Port)
	log.Printf("[%s] Initial seeds: %v", node.ID, seeds)
	log.Printf("[%s] Ring fingerprint: %08x", node.ID, node.ring.Fingerprint)

	return node
}

// getHeartbeat returns this node's current heartbeat.
func (n *Node) getHeartbeat() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.heartbeat
}

// incrementHeartbeat increments this node's heartbeat counter.
func (n *Node) incrementHeartbeat() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.heartbeat++
	return n.heartbeat
}

// getPeerInfosWithSelf returns all known peers plus self as PeerInfo slice.
func (n *Node) getPeerInfosWithSelf() []cluster.PeerInfo {
	infos := n.peers.GetPeerInfos()
	// Add self
	infos = append(infos, cluster.PeerInfo{
		ID:        n.ID,
		Addr:      n.Addr,
		Heartbeat: n.getHeartbeat(),
	})
	return infos
}

// toProtoPeers converts cluster.PeerInfo slice to proto PeerInfo slice.
func toProtoPeers(infos []cluster.PeerInfo) []*pb.PeerInfo {
	result := make([]*pb.PeerInfo, len(infos))
	for i, info := range infos {
		result[i] = &pb.PeerInfo{
			Id:        info.ID,
			Addr:      info.Addr,
			Heartbeat: info.Heartbeat,
		}
	}
	return result
}

// fromProtoPeers converts proto PeerInfo slice to cluster.PeerInfo slice.
func fromProtoPeers(peers []*pb.PeerInfo) []cluster.PeerInfo {
	result := make([]cluster.PeerInfo, len(peers))
	for i, p := range peers {
		result[i] = cluster.PeerInfo{
			ID:        p.Id,
			Addr:      p.Addr,
			Heartbeat: p.Heartbeat,
		}
	}
	return result
}

// maybeRebalanceRing rebalances the ring if membership changed.
func (n *Node) maybeRebalanceRing() {
	// Get alive peer IDs + self
	aliveIDs := n.peers.GetAliveIDs()

	// Add self if not already included
	hasSelf := false
	for _, id := range aliveIDs {
		if id == n.ID {
			hasSelf = true
			break
		}
	}
	if !hasSelf {
		aliveIDs = append(aliveIDs, n.ID)
	}

	// Rebalance
	n.ring.Rebalance(aliveIDs)
	log.Printf("[%s] Ring rebalanced, fingerprint: %08x, nodes: %v", n.ID, n.ring.Fingerprint, aliveIDs)
}

// Ping RPC - exchanges peer info for gossip
func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Received Ping from %s with %d nodes", n.ID, req.NodeId, len(req.Peers))

	// Convert and merge incoming peers
	incoming := fromProtoPeers(req.Peers)
	merged, newPeerDiscovered := n.peers.MergePeers(incoming)

	// Add self to merged
	merged = append(merged, cluster.PeerInfo{
		ID:        n.ID,
		Addr:      n.Addr,
		Heartbeat: n.getHeartbeat(),
	})

	// Rebalance ring if new peer discovered
	if newPeerDiscovered {
		n.maybeRebalanceRing()
	}

	return &pb.PingResponse{
		NodeId:    n.ID,
		Timestamp: time.Now().Unix(),
		Peers:     toProtoPeers(merged),
	}, nil
}

// Fetch RPC - returns the owner of a key
func (n *Node) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	ownerID, err := n.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	ownerAddr := n.peers.GetAddress(ownerID)
	if ownerID == n.ID {
		ownerAddr = n.Addr
	}

	log.Printf("[%s] Fetch key=%s, owner=%s", n.ID, req.Key, ownerID)

	return &pb.FetchResponse{
		OwnerId:         ownerID,
		OwnerAddr:       ownerAddr,
		RingFingerprint: n.ring.Fingerprint,
	}, nil
}

// Put RPC - stores a value if this node owns the key, otherwise returns redirect info
func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	ownerID, err := n.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	// Check if we own this key
	if ownerID != n.ID {
		ownerAddr := n.peers.GetAddress(ownerID)
		log.Printf("[%s] Put key=%s redirect to %s", n.ID, req.Key, ownerID)
		return &pb.PutResponse{
			Stored:    false,
			OwnerId:   ownerID,
			OwnerAddr: ownerAddr,
		}, nil
	}

	// We own it - store the value
	n.dataMu.Lock()
	n.data[req.Key] = req.Value
	n.dataMu.Unlock()

	log.Printf("[%s] Put key=%s stored (%d bytes)", n.ID, req.Key, len(req.Value))

	return &pb.PutResponse{
		Stored: true,
	}, nil
}

// Get RPC - retrieves a value if this node owns the key, otherwise returns redirect info
func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	ownerID, err := n.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	// Check if we own this key
	if ownerID != n.ID {
		ownerAddr := n.peers.GetAddress(ownerID)
		log.Printf("[%s] Get key=%s redirect to %s", n.ID, req.Key, ownerID)
		return &pb.GetResponse{
			Found:     false,
			OwnerId:   ownerID,
			OwnerAddr: ownerAddr,
		}, nil
	}

	// We own it - look up the value
	n.dataMu.RLock()
	value, found := n.data[req.Key]
	n.dataMu.RUnlock()

	log.Printf("[%s] Get key=%s found=%v", n.ID, req.Key, found)

	return &pb.GetResponse{
		Found: found,
		Value: value,
	}, nil
}

// Echo RPC
func (n *Node) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	log.Printf("[%s] Received Echo from %s: '%s' (hop %d)",
		n.ID, req.NodeId, req.Message, req.HopCount)

	return &pb.EchoResponse{
		NodeId:   n.ID,
		Message:  fmt.Sprintf("echo from %s: %s", n.ID, req.Message),
		HopCount: req.HopCount + 1,
	}, nil
}

// StartGossip begins the background gossip loop.
// It periodically pings a random peer to exchange peer info.
// Cancel the context to stop the loop.
func (n *Node) StartGossip(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[%s] Starting gossip with interval %v", n.ID, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Gossip stopped", n.ID)
			return
		case <-ticker.C:
			n.gossipOnce()
		}
	}
}

// gossipOnce increments heartbeat, picks a random peer, and exchanges peer info.
func (n *Node) gossipOnce() {
	// Step 1: Increment our own heartbeat
	newHb := n.incrementHeartbeat()
	log.Printf("[%s] Heartbeat: %d", n.ID, newHb)

	// Step 2: Pick a random peer
	peerID, peerAddr := n.pickRandomPeer()
	if peerAddr == "" {
		return // no peers to gossip with
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Step 3: Connect to peer
	conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] Failed to connect to %s: %v", n.ID, peerAddr, err)
		if n.peers.IncrementStaleCount(peerID) {
			n.maybeRebalanceRing()
		}
		return
	}
	defer conn.Close()

	client := pb.NewNodeServiceClient(conn)

	// Step 4: Send ping with our peer infos
	resp, err := client.Ping(ctx, &pb.PingRequest{
		NodeId: n.ID,
		Peers:  toProtoPeers(n.getPeerInfosWithSelf()),
	})
	if err != nil {
		log.Printf("[%s] Ping to %s failed: %v", n.ID, peerAddr, err)
		if n.peers.IncrementStaleCount(peerID) {
			n.maybeRebalanceRing()
		}
		return
	}

	// Step 5: Process response - update peer info and stale counts
	responsePeers := fromProtoPeers(resp.Peers)
	peerMarkedDead := n.peers.ProcessResponse(responsePeers)

	if peerMarkedDead {
		n.maybeRebalanceRing()
	}

	log.Printf("[%s] Gossiped with %s, now know %d peers (%d alive)",
		n.ID, peerID, len(n.peers.GetAll()), len(n.peers.GetAlive()))
}

// pickRandomPeer returns a random peer ID and address, excluding self.
func (n *Node) pickRandomPeer() (string, string) {
	peers := n.peers.GetAliveForGossip() // includes unresolved seeds
	candidates := make([]cluster.Peer, 0, len(peers))

	for _, p := range peers {
		if p.ID != n.ID && p.Addr != n.Addr {
			candidates = append(candidates, p)
		}
	}

	if len(candidates) == 0 {
		return "", ""
	}

	chosen := candidates[rand.Intn(len(candidates))]
	return chosen.ID, chosen.Addr
}
