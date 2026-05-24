package kvcluster

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/chorus/cluster"
	"github.com/chorus/hashes"
	pb "github.com/chorus/proto"
	"github.com/chorus/ring"
	"github.com/chorus/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const ServiceName = "kv"
const ReplicationFactor = 3

type KVCluster struct {
	pb.UnimplementedKVServiceServer

	ID      string
	Addr    string
	cluster *cluster.Server
	ring    *ring.Ring
	store   storage.Store

	connMu sync.RWMutex
	conns  map[string]*grpc.ClientConn
}

func New(c *cluster.Server, addr string, store storage.Store) *KVCluster {
	kc := &KVCluster{
		ID:      c.ID,
		Addr:    addr,
		cluster: c,
		ring:    ring.New(hashes.HashViaCRC32),
		store:   store,
		conns:   map[string]*grpc.ClientConn{},
	}
	c.RegisterService(ServiceName, addr)
	kc.ring.Rebalance([]string{c.ID})
	c.OnMembershipChange(kc.rebalanceRing)
	log.Printf("[%s] KVCluster ready on %s, ring fingerprint: %08x", kc.ID, addr, kc.ring.Fingerprint)
	return kc
}

func (kc *KVCluster) rebalanceRing() {
	peers := kc.cluster.Peers().GetByService(ServiceName)
	ids := make([]string, 0, len(peers)+1)
	ids = append(ids, kc.ID)
	for _, p := range peers {
		ids = append(ids, p.ID)
	}
	kc.ring.Rebalance(ids)
	log.Printf("[%s] Ring rebalanced, fingerprint: %08x, nodes: %v", kc.ID, kc.ring.Fingerprint, ids)
}

func (kc *KVCluster) kvAddr(nodeID string) string {
	if nodeID == kc.ID {
		return kc.Addr
	}
	return kc.cluster.Peers().GetServiceAddr(nodeID, ServiceName)
}

func (kc *KVCluster) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	ownerID, err := kc.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	ownerAddr := kc.kvAddr(ownerID)

	log.Printf("[%s] Fetch key=%s, owner=%s", kc.ID, req.Key, ownerID)

	return &pb.FetchResponse{
		OwnerId:         ownerID,
		OwnerAddr:       ownerAddr,
		RingFingerprint: kc.ring.Fingerprint,
	}, nil
}

func (kc *KVCluster) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	replicas, err := kc.ring.GetNodes(req.Key, ReplicationFactor)
	if err != nil {
		return nil, err
	}

	primaryID := replicas[0]

	if primaryID != kc.ID {
		return &pb.PutResponse{
			Stored:    false,
			OwnerId:   primaryID,
			OwnerAddr: kc.kvAddr(primaryID),
		}, nil
	}

	log.Printf("[%s] Put key=%s - I'm primary, replicas: %v", kc.ID, req.Key, replicas)

	replicaStatuses := make([]*pb.ReplicaStatus, 0, len(replicas))
	allSuccess := true

	if len(replicas) > 1 {
		type replicaResult struct {
			nodeID  string
			success bool
			err     string
		}

		results := make(chan replicaResult, len(replicas)-1)

		for _, replicaID := range replicas[1:] {
			go func(nodeID string) {
				success, errMsg := kc.replicateToNode(ctx, nodeID, req.Key, req.Value)
				results <- replicaResult{nodeID: nodeID, success: success, err: errMsg}
			}(replicaID)
		}

		for i := 0; i < len(replicas)-1; i++ {
			result := <-results
			replicaStatuses = append(replicaStatuses, &pb.ReplicaStatus{
				NodeId:  result.nodeID,
				Success: result.success,
				Error:   result.err,
			})
			if !result.success {
				allSuccess = false
			}
		}
	}

	if !allSuccess {
		log.Printf("[%s] Put key=%s failed - not all replicas acknowledged", kc.ID, req.Key)
		return &pb.PutResponse{
			Stored:        false,
			ReplicaStatus: replicaStatuses,
		}, nil
	}

	if err := kc.store.Insert([]byte(req.Key), req.Value); err != nil {
		return nil, err
	}

	replicaStatuses = append([]*pb.ReplicaStatus{{
		NodeId:  kc.ID,
		Success: true,
	}}, replicaStatuses...)

	log.Printf("[%s] Put key=%s stored on all %d replicas", kc.ID, req.Key, len(replicas))

	return &pb.PutResponse{
		Stored:        true,
		ReplicaStatus: replicaStatuses,
	}, nil
}

func (kc *KVCluster) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	ownerID, err := kc.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	if ownerID != kc.ID {
		return &pb.GetResponse{
			Found:     false,
			OwnerId:   ownerID,
			OwnerAddr: kc.kvAddr(ownerID),
		}, nil
	}

	value, err := kc.store.Get([]byte(req.Key))
	if err != nil {
		return nil, err
	}

	log.Printf("[%s] Get key=%s found=%v", kc.ID, req.Key, value != nil)

	return &pb.GetResponse{
		Found: value != nil,
		Value: value,
	}, nil
}

func (kc *KVCluster) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if err := kc.store.Insert([]byte(req.Key), req.Value); err != nil {
		return nil, err
	}
	log.Printf("[%s] Replicate key=%s", kc.ID, req.Key)
	return &pb.ReplicateResponse{Stored: true}, nil
}

func (kc *KVCluster) replicateToNode(ctx context.Context, nodeID, key string, value []byte) (bool, string) {
	addr := kc.kvAddr(nodeID)
	if addr == "" {
		return false, "unknown node address"
	}

	conn, err := kc.getConn(addr)
	if err != nil {
		return false, fmt.Sprintf("connection failed: %v", err)
	}

	client := pb.NewKVServiceClient(conn)
	resp, err := client.Replicate(ctx, &pb.ReplicateRequest{Key: key, Value: value})
	if err != nil {
		return false, fmt.Sprintf("replicate RPC failed: %v", err)
	}
	return resp.Stored, ""
}

func (kc *KVCluster) getConn(addr string) (*grpc.ClientConn, error) {
	kc.connMu.RLock()
	conn, ok := kc.conns[addr]
	kc.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	kc.connMu.Lock()
	defer kc.connMu.Unlock()
	if conn, ok := kc.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	kc.conns[addr] = conn
	return conn, nil
}
