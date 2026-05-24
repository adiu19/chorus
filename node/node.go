package node

import (
	"context"
	"log"
	"net"
	"path/filepath"
	"time"

	"github.com/chorus/cluster"
	"github.com/chorus/kvcluster"
	pb "github.com/chorus/proto"
	"github.com/chorus/scheduler/core"
	"github.com/chorus/storage"
	"github.com/chorus/storage/lsm"
	"github.com/chorus/storage/memstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type StoreKind int

const (
	StoreMem StoreKind = iota
	StoreLSM
)

type KVClusterConfig struct {
	Store    StoreKind
	DataPath string
}

type SchedulerConfig struct {
	CapacityPerWorker int
	TickInterval      time.Duration
	MaxPendingJobs    int
}

type Config struct {
	ID    string
	Port  string
	Seeds []string

	GossipInterval time.Duration

	KV        *KVClusterConfig
	Scheduler *SchedulerConfig
}

type Node struct {
	Config Config
	Addr   string

	Cluster   *cluster.Server
	KV        *kvcluster.KVCluster
	Scheduler *core.Scheduler
	SchedSrv  *core.Server

	grpcSrv *grpc.Server
}

func New(cfg Config) *Node {
	addr := "localhost:" + cfg.Port

	n := &Node{
		Config: cfg,
		Addr:   addr,
	}

	n.Cluster = cluster.NewServer(cfg.ID, addr, cfg.Seeds)

	if cfg.KV != nil {
		store, err := buildStore(cfg.KV.Store, cfg.KV.DataPath, cfg.ID)
		if err != nil {
			log.Fatalf("[%s] Failed to init kv store: %v", cfg.ID, err)
		}
		n.KV = kvcluster.New(n.Cluster, addr, store)
	}

	if cfg.Scheduler != nil {
		n.Scheduler = core.New(core.Config{
			CapacityPerWorker: cfg.Scheduler.CapacityPerWorker,
			TickInterval:      cfg.Scheduler.TickInterval,
			MaxPendingJobs:    cfg.Scheduler.MaxPendingJobs,
		})
		n.SchedSrv = core.NewServer(n.Scheduler)
	}

	return n
}

func buildStore(kind StoreKind, dataPath, defaultID string) (storage.Store, error) {
	switch kind {
	case StoreMem:
		return memstore.NewMemStore()
	case StoreLSM:
		if dataPath == "" {
			dataPath = filepath.Join("data", "nodes", defaultID)
		}
		return lsm.NewLSM(dataPath)
	}
	return memstore.NewMemStore()
}

func (n *Node) Start(ctx context.Context) error {
	if n.Scheduler != nil {
		n.Scheduler.Start()
	}

	n.grpcSrv = grpc.NewServer()
	pb.RegisterClusterServiceServer(n.grpcSrv, n.Cluster)
	if n.KV != nil {
		pb.RegisterKVServiceServer(n.grpcSrv, n.KV)
	}
	if n.SchedSrv != nil {
		pb.RegisterSchedulerServiceServer(n.grpcSrv, n.SchedSrv)
	}
	reflection.Register(n.grpcSrv)

	lis, err := net.Listen("tcp", ":"+n.Config.Port)
	if err != nil {
		return err
	}
	log.Printf("[%s] gRPC server listening on :%s", n.Config.ID, n.Config.Port)
	go n.grpcSrv.Serve(lis)

	interval := n.Config.GossipInterval
	if interval == 0 {
		interval = 5 * time.Second
	}
	go n.Cluster.Start(ctx, interval)

	return nil
}

func (n *Node) Stop() {
	if n.grpcSrv != nil {
		n.grpcSrv.GracefulStop()
	}
	if n.Scheduler != nil {
		n.Scheduler.Stop()
	}
}
