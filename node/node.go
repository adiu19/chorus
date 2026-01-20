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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	ID   string
	Port string
	Addr string // "localhost:port" - this node's address

	peers *cluster.PeerList

	mu        sync.RWMutex
	heartbeat int64 // this node's heartbeat counter, only we increment this
}

func NewNode(id, port string, seeds []string) *Node {
	addr := "localhost:" + port

	node := &Node{
		ID:        id,
		Port:      port,
		Addr:      addr,
		peers:     cluster.NewPeerList(seeds),
		heartbeat: 0,
	}

	log.Printf("[%s] Node starting on :%s", node.ID, node.Port)
	log.Printf("[%s] Initial peers: %v", node.ID, node.peers.GetAddresses())

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

// getHeartbeatsWithSelf returns all known heartbeats including our own.
func (n *Node) getHeartbeatsWithSelf() map[string]int64 {
	heartbeats := n.peers.GetHeartbeats()
	heartbeats[n.Addr] = n.getHeartbeat()
	return heartbeats
}

// Ping RPC - exchanges heartbeat maps for gossip
func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Received Ping from %s with %d heartbeats", n.ID, req.NodeId, len(req.Heartbeats))

	// Merge incoming heartbeats (take max) and get merged result
	merged := n.peers.MergeHeartbeats(req.Heartbeats)

	// Add our own heartbeat to the response
	merged[n.Addr] = n.getHeartbeat()

	return &pb.PingResponse{
		NodeId:     n.ID,
		Timestamp:  time.Now().Unix(),
		Heartbeats: merged,
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
// It periodically pings a random peer to exchange heartbeats.
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

// gossipOnce increments heartbeat, picks a random peer, and exchanges heartbeats.
func (n *Node) gossipOnce() {
	// Step 1: Increment our own heartbeat
	newHb := n.incrementHeartbeat()
	log.Printf("[%s] Heartbeat: %d", n.ID, newHb)

	// Step 2: Pick a random peer
	peer := n.pickRandomPeer()
	if peer == "" {
		return // no peers to gossip with
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Step 3: Connect to peer
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] Failed to connect to %s: %v", n.ID, peer, err)
		n.peers.IncrementStaleCount(peer)
		return
	}
	defer conn.Close()

	client := pb.NewNodeServiceClient(conn)

	// Step 4: Send ping with our heartbeats
	resp, err := client.Ping(ctx, &pb.PingRequest{
		NodeId:     n.ID,
		Heartbeats: n.getHeartbeatsWithSelf(),
	})
	if err != nil {
		log.Printf("[%s] Ping to %s failed: %v", n.ID, peer, err)
		n.peers.IncrementStaleCount(peer)
		return
	}

	// Step 5: Process response - update heartbeats and stale counts
	n.peers.ProcessResponse(resp.Heartbeats)

	log.Printf("[%s] Gossiped with %s, now know %d peers (%d alive)",
		n.ID, peer, len(n.peers.GetAddresses()), len(n.peers.GetAlive()))
}

// pickRandomPeer returns a random peer address, excluding self.
func (n *Node) pickRandomPeer() string {
	addrs := n.peers.GetAddresses()
	candidates := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr != n.Addr {
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	return candidates[rand.Intn(len(candidates))]
}
