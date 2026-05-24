package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chorus/config"
	"github.com/chorus/node"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	storeKind := node.StoreLSM
	if getEnv("STORE", "lsm") == "mem" {
		storeKind = node.StoreMem
	}

	n := node.New(node.Config{
		ID:    getEnv("NODE_ID", "node1"),
		Port:  getEnv("PORT", "8010"),
		Seeds: cfg.Seeds,
		KV: &node.KVClusterConfig{
			Store: storeKind,
		},
		Scheduler: &node.SchedulerConfig{
			CapacityPerWorker: 10,
			TickInterval:      500 * time.Millisecond,
			MaxPendingJobs:    1000,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := n.Start(ctx); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("Shutting down gracefully...")
	cancel()
	n.Stop()
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
