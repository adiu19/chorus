package node

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/chorus/cluster"
	pb "github.com/chorus/proto"
	"github.com/chorus/ring"
	"github.com/chorus/scheduler"
	"github.com/chorus/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ReplicationFactor is the number of nodes that store each key
const ReplicationFactor = 3

type Node struct {
	pb.UnimplementedNodeServiceServer
	ID   string
	Port string
	Addr string // "localhost:port" - this node's address

	peers     *cluster.PeerList
	ring      *ring.Ring
	wal       *wal.WAL
	scheduler *scheduler.Scheduler

	mu        sync.RWMutex
	heartbeat int64 // this node's heartbeat counter, only we increment this

	dataMu sync.RWMutex
	data   map[string][]byte // key-value storage

	connMu sync.RWMutex
	conns  map[string]*grpc.ClientConn // addr -> persistent connection
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
		conns:     make(map[string]*grpc.ClientConn),
	}

	// Initialize WAL
	walDir := filepath.Join("data", "nodes", id)
	w, err := wal.Open(walDir)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	node.wal = w

	// Replay WAL entries
	node.replayWAL()

	// Add seeds (we don't know their IDs yet, use address as temp ID)
	// Skip our own address
	for _, seedAddr := range seeds {
		if seedAddr != addr {
			node.peers.AddSeedAddr(seedAddr)
		}
	}

	// Initialize scheduler
	node.scheduler = scheduler.New(scheduler.Config{
		CapacityPerWorker: 10,
		TickInterval:      500 * time.Millisecond,
		MaxPendingJobs:    100,
	})
	node.scheduler.Start()

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

// replayWAL replays all WAL entries to restore state.
func (n *Node) replayWAL() {
	entries, _ := n.wal.ReadAll()
	for _, e := range entries {
		if e.Op == "put" {
			n.data[e.Key] = e.Value
		}
	}
	if len(entries) > 0 {
		n.wal.SetIndex(entries[len(entries)-1].Seq)
		log.Printf("[%s] Replayed %d WAL entries", n.ID, len(entries))
	}
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

// Put RPC - stores a value if this node is primary, replicates to other nodes
func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Get all replica nodes for this key
	replicas, err := n.ring.GetNodes(req.Key, ReplicationFactor)
	if err != nil {
		return nil, err
	}

	// Primary is the first node in the replica list
	primaryID := replicas[0]

	// Check if we're the primary
	if primaryID != n.ID {
		primaryAddr := n.peers.GetAddress(primaryID)
		log.Printf("[%s] Put key=%s redirect to primary %s", n.ID, req.Key, primaryID)
		return &pb.PutResponse{
			Stored:    false,
			OwnerId:   primaryID,
			OwnerAddr: primaryAddr,
		}, nil
	}

	// We're the primary - replicate to other nodes first
	log.Printf("[%s] Put key=%s - I'm primary, replicas: %v", n.ID, req.Key, replicas)

	replicaStatuses := make([]*pb.ReplicaStatus, 0, len(replicas))
	allSuccess := true

	// Replicate to secondary nodes in parallel
	if len(replicas) > 1 {
		type replicaResult struct {
			nodeID  string
			success bool
			err     string
		}

		results := make(chan replicaResult, len(replicas)-1)

		for _, replicaID := range replicas[1:] {
			go func(nodeID string) {
				success, errMsg := n.replicateToNode(ctx, nodeID, req.Key, req.Value)
				results <- replicaResult{nodeID: nodeID, success: success, err: errMsg}
			}(replicaID)
		}

		// Collect results
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

	// Only store locally if all replicas succeeded (write-all-N semantics)
	if !allSuccess {
		log.Printf("[%s] Put key=%s failed - not all replicas acknowledged", n.ID, req.Key)
		return &pb.PutResponse{
			Stored:        false,
			ReplicaStatus: replicaStatuses,
		}, nil
	}

	// All replicas succeeded - write to WAL and store locally
	seq, err := n.wal.Append("primary", "put", req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	n.dataMu.Lock()
	n.data[req.Key] = req.Value
	n.dataMu.Unlock()

	n.wal.SetIndex(seq)

	// Add self to replica status
	replicaStatuses = append([]*pb.ReplicaStatus{{
		NodeId:  n.ID,
		Success: true,
	}}, replicaStatuses...)

	log.Printf("[%s] Put key=%s stored on all %d replicas", n.ID, req.Key, len(replicas))

	return &pb.PutResponse{
		Stored:        true,
		ReplicaStatus: replicaStatuses,
	}, nil
}

// replicateToNode sends a Replicate RPC to a specific node
func (n *Node) replicateToNode(ctx context.Context, nodeID, key string, value []byte) (bool, string) {
	addr := n.peers.GetAddress(nodeID)
	if addr == "" {
		return false, "unknown node address"
	}

	conn, err := n.getConn(addr)
	if err != nil {
		return false, fmt.Sprintf("connection failed: %v", err)
	}

	client := pb.NewNodeServiceClient(conn)

	resp, err := client.Replicate(ctx, &pb.ReplicateRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return false, fmt.Sprintf("replicate RPC failed: %v", err)
	}

	return resp.Stored, ""
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

// Replicate RPC - stores a value directly without ownership check (used by primary)
func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	seq, err := n.wal.Append("replica", "put", req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	n.dataMu.Lock()
	n.data[req.Key] = req.Value
	n.dataMu.Unlock()

	n.wal.SetIndex(seq)

	log.Printf("[%s] Replicate key=%s seq=%d", n.ID, req.Key, seq)

	return &pb.ReplicateResponse{
		Stored: true,
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

// SubmitJob RPC - validates and enqueues a job for scheduling
func (n *Node) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	// Validate required fields
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "job ID is required")
	}
	if req.Cost <= 0 {
		return nil, status.Error(codes.InvalidArgument, "cost must be positive")
	}
	if req.DurationMs <= 0 {
		return nil, status.Error(codes.InvalidArgument, "duration_ms must be positive")
	}

	job := &scheduler.Job{
		ID:       req.Id,
		Priority: int(req.Priority),
		Cost:     int(req.Cost),
		Duration: time.Duration(req.DurationMs) * time.Millisecond,
	}

	if err := n.scheduler.Submit(job); err != nil {
		log.Printf("[%s] SubmitJob id=%s rejected: %v", n.ID, req.Id, err)
		return &pb.SubmitJobResponse{
			Id:     req.Id,
			Status: "rejected",
			Reason: err.Error(),
		}, nil
	}

	log.Printf("[%s] SubmitJob id=%s priority=%d cost=%d duration=%dms",
		n.ID, req.Id, req.Priority, req.Cost, req.DurationMs)

	return &pb.SubmitJobResponse{
		Id:     req.Id,
		Status: "pending",
	}, nil
}

// GetJobStatus RPC - returns the current status of a job
func (n *Node) GetJobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	job, ok := n.scheduler.GetJob(req.Id)
	if !ok {
		return &pb.JobStatusResponse{
			Id:     req.Id,
			Status: "unknown",
		}, nil
	}

	return &pb.JobStatusResponse{
		Id:       job.ID,
		Status:   job.Status.String(),
		WorkerId: job.WorkerID,
		Priority: int32(job.Priority),
		Cost:     int32(job.Cost),
	}, nil
}

// ListJobs RPC - returns a summary of all jobs known to the scheduler
func (n *Node) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	jobs := n.scheduler.GetAllJobs()
	summaries := make([]*pb.JobSummary, len(jobs))
	for i, job := range jobs {
		summaries[i] = &pb.JobSummary{
			Id:       job.ID,
			Status:   job.Status.String(),
			Priority: int32(job.Priority),
			Cost:     int32(job.Cost),
			WorkerId: job.WorkerID,
		}
	}

	return &pb.ListJobsResponse{
		Jobs: summaries,
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

// getConn returns a cached connection or creates a new one.
func (n *Node) getConn(addr string) (*grpc.ClientConn, error) {
	n.connMu.RLock()
	conn, ok := n.conns[addr]
	n.connMu.RUnlock()

	if ok {
		return conn, nil
	}

	// Create new connection
	n.connMu.Lock()
	defer n.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := n.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	n.conns[addr] = conn
	return conn, nil
}

// closeConn removes and closes a connection (e.g., when peer is dead).
func (n *Node) closeConn(addr string) {
	n.connMu.Lock()
	defer n.connMu.Unlock()

	if conn, ok := n.conns[addr]; ok {
		conn.Close()
		delete(n.conns, addr)
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

	// Step 3: Get or create connection to peer
	conn, err := n.getConn(peerAddr)
	if err != nil {
		log.Printf("[%s] Failed to connect to %s: %v", n.ID, peerAddr, err)
		if n.peers.IncrementStaleCount(peerID) {
			n.maybeRebalanceRing()
		}
		return
	}

	client := pb.NewNodeServiceClient(conn)

	// Step 4: Send ping with our peer infos
	resp, err := client.Ping(ctx, &pb.PingRequest{
		NodeId: n.ID,
		Peers:  toProtoPeers(n.getPeerInfosWithSelf()),
	})
	if err != nil {
		log.Printf("[%s] Ping to %s failed: %v", n.ID, peerAddr, err)
		n.closeConn(peerAddr) // close bad connection
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
