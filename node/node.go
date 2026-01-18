package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/chorus/node/proto"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	id    string
	port  string
	seeds []string
}

func NewNode(seeds []string) *Node {
	node := &Node{
		id:    getEnv("NODE_ID", "node1"),
		port:  getEnv("PORT", "8001"),
		seeds: seeds,
	}

	log.Printf("[%s] Node starting on :%s", node.id, node.port)
	log.Printf("[%s] Seeds: %v", node.id, node.seeds)

	return node
}

// Ping RPC
func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Received Ping from %s", n.id, req.NodeId)

	return &pb.PingResponse{
		NodeId:    n.id,
		Timestamp: time.Now().Unix(),
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

// getEnv returns the environment variable value or a default.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
