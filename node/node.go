package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/chorus/node/cluster"
	pb "github.com/chorus/node/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	id    string
	port  string
	peers *cluster.PeerList
}

func NewNode(seeds []string) *Node {
	node := &Node{
		id:    getEnv("NODE_ID", "node1"),
		port:  getEnv("PORT", "8001"),
		peers: cluster.NewPeerList(seeds),
	}

	log.Printf("[%s] Node starting on :%s", node.id, node.port)
	log.Printf("[%s] Initial peers: %v", node.id, node.peers.GetAddresses())

	return node
}

// Ping RPC - also exchanges peer lists for gossip
func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Received Ping from %s with %d peers", n.id, req.NodeId, len(req.KnownPeers))

	// Merge incoming peers into our list
	for _, addr := range req.KnownPeers {
		n.peers.Add(addr)
	}

	return &pb.PingResponse{
		NodeId:     n.id,
		Timestamp:  time.Now().Unix(),
		KnownPeers: n.peers.GetAddresses(),
	}, nil
}

// Echo RPC
func (n *Node) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	log.Printf("[%s] Received Echo from %s: '%s' (hop %d)",
		n.id, req.NodeId, req.Message, req.HopCount)

	return &pb.EchoResponse{
		NodeId:   n.id,
		Message:  fmt.Sprintf("echo from %s: %s", n.id, req.Message),
		HopCount: req.HopCount + 1,
	}, nil
}

// StartGossip begins the background gossip loop.
// It periodically pings a random peer to exchange peer lists.
// Cancel the context to stop the loop.
func (n *Node) StartGossip(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[%s] Starting gossip with interval %v", n.id, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Gossip stopped", n.id)
			return
		case <-ticker.C:
			n.gossipOnce()
		}
	}
}

// gossipOnce picks a random peer and exchanges peer lists.
func (n *Node) gossipOnce() {
	peer := n.pickRandomPeer()
	if peer == "" {
		return // no peers to gossip with
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Connect to peer
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] Failed to connect to %s: %v", n.id, peer, err)
		n.peers.MarkDead(peer)
		return
	}
	defer conn.Close()

	client := pb.NewNodeServiceClient(conn)

	// Send ping with our known peers
	resp, err := client.Ping(ctx, &pb.PingRequest{
		NodeId:     n.id,
		KnownPeers: n.peers.GetAddresses(),
	})
	if err != nil {
		log.Printf("[%s] Ping to %s failed: %v", n.id, peer, err)
		n.peers.MarkDead(peer)
		return
	}

	// Merge response peers and mark this peer alive
	n.peers.MarkAlive(peer)
	for _, addr := range resp.KnownPeers {
		n.peers.Add(addr)
	}

	log.Printf("[%s] Gossiped with %s, now know %d peers", n.id, peer, len(n.peers.GetAddresses()))
}

// pickRandomPeer returns a random peer address, excluding self.
func (n *Node) pickRandomPeer() string {
	selfAddr := "localhost:" + n.port

	addrs := n.peers.GetAddresses()
	candidates := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr != selfAddr {
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	return candidates[rand.Intn(len(candidates))]
}

// getEnv returns the environment variable value or a default.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
