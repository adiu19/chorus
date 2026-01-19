package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chorus/config"
	"github.com/chorus/node"
	pb "github.com/chorus/proto"
	"google.golang.org/grpc"
)

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	n := node.NewNode(getEnv("NODE_ID", "node1"), getEnv("PORT", "8010"), cfg.Seeds)

	lis, err := net.Listen("tcp", ":"+n.Port)
	if err != nil {
		log.Fatalf("[%s] Failed to listen: %v", n.ID, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, n)

	log.Printf("[%s] gRPC server listening on :%s", n.ID, n.Port)

	// Start gossip in background
	ctx, cancel := context.WithCancel(context.Background())
	go n.StartGossip(ctx, 5*time.Second)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("[%s] Shutting down gracefully...", n.ID)
		cancel() // stop gossip
		grpcServer.GracefulStop()
	}()

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Failed to serve: %v", n.ID, err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
