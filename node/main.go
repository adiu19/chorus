package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/chorus/node/proto"
	"google.golang.org/grpc"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	id   string
	port string
}

func NewNode() *Node {
	node := &Node{
		id:   getEnv("NODE_ID", "node1"),
		port: getEnv("PORT", "8001"),
	}

	log.Printf("[%s] Node starting on :%s", node.id, node.port)

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

func main() {
	node := NewNode()

	lis, err := net.Listen("tcp", ":"+node.port)
	if err != nil {
		log.Fatalf("[%s] Failed to listen: %v", node.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, node)

	log.Printf("[%s] gRPC server listening on :%s", node.id, node.port)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("[%s] Shutting down gracefully...", node.id)
		grpcServer.GracefulStop()
	}()

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Failed to serve: %v", node.id, err)
	}
}

// get environment variable with defaults
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
