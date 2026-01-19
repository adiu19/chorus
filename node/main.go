package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chorus/node/config"
	pb "github.com/chorus/node/proto"
	"google.golang.org/grpc"
)

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	node := NewNode(cfg.Seeds)

	lis, err := net.Listen("tcp", ":"+node.port)
	if err != nil {
		log.Fatalf("[%s] Failed to listen: %v", node.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, node)

	log.Printf("[%s] gRPC server listening on :%s", node.id, node.port)

	// Start gossip in background
	ctx, cancel := context.WithCancel(context.Background())
	go node.StartGossip(ctx, 5*time.Second)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("[%s] Shutting down gracefully...", node.id)
		cancel() // stop gossip
		grpcServer.GracefulStop()
	}()

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Failed to serve: %v", node.id, err)
	}
}
