package cluster

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/chorus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedClusterServiceServer

	ID   string
	Addr string

	peers *PeerList

	mu        sync.RWMutex
	heartbeat int64
	services  map[string]string
	onChange  []func()

	connMu sync.RWMutex
	conns  map[string]*grpc.ClientConn
}

func NewServer(id, addr string, seeds []string) *Server {
	s := &Server{
		ID:       id,
		Addr:     addr,
		peers:    NewPeerList(id, addr),
		services: map[string]string{},
		conns:    map[string]*grpc.ClientConn{},
	}
	for _, seedAddr := range seeds {
		if seedAddr != addr {
			s.peers.AddSeedAddr(seedAddr)
		}
	}
	return s
}

func (s *Server) RegisterService(name, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[name] = addr
}

func (s *Server) OnMembershipChange(cb func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = append(s.onChange, cb)
}

func (s *Server) Peers() *PeerList {
	return s.peers
}

func (s *Server) Heartbeat() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.heartbeat
}

func (s *Server) selfInfo() PeerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return PeerInfo{
		ID:        s.ID,
		Addr:      s.Addr,
		Heartbeat: s.heartbeat,
		Services:  copyServices(s.services),
	}
}

func (s *Server) incrementHeartbeat() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeat++
	return s.heartbeat
}

func (s *Server) notifyChange() {
	s.mu.RLock()
	cbs := make([]func(), len(s.onChange))
	copy(cbs, s.onChange)
	s.mu.RUnlock()
	for _, cb := range cbs {
		cb()
	}
}

func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Received Ping from %s with %d nodes", s.ID, req.NodeId, len(req.Peers))

	incoming := fromProtoPeers(req.Peers)
	merged, newPeerDiscovered := s.peers.MergePeers(incoming)
	merged = append(merged, s.selfInfo())

	if newPeerDiscovered {
		s.notifyChange()
	}

	return &pb.PingResponse{
		NodeId:    s.ID,
		Timestamp: time.Now().Unix(),
		Peers:     toProtoPeers(merged),
	}, nil
}

func (s *Server) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	log.Printf("[%s] Received Echo from %s: '%s' (hop %d)", s.ID, req.NodeId, req.Message, req.HopCount)
	return &pb.EchoResponse{
		NodeId:   s.ID,
		Message:  "echo from " + s.ID + ": " + req.Message,
		HopCount: req.HopCount + 1,
	}, nil
}

func (s *Server) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[%s] Starting gossip with interval %v", s.ID, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Gossip stopped", s.ID)
			return
		case <-ticker.C:
			s.gossipOnce()
		}
	}
}

func (s *Server) gossipOnce() {
	newHb := s.incrementHeartbeat()
	log.Printf("[%s] Heartbeat: %d", s.ID, newHb)

	peerID, peerAddr := s.pickRandomPeer()
	if peerAddr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := s.getConn(peerAddr)
	if err != nil {
		log.Printf("[%s] Failed to connect to %s: %v", s.ID, peerAddr, err)
		if s.peers.IncrementStaleCount(peerID) {
			s.notifyChange()
		}
		return
	}

	client := pb.NewClusterServiceClient(conn)

	infos := s.peers.GetPeerInfos()
	infos = append(infos, s.selfInfo())

	resp, err := client.Ping(ctx, &pb.PingRequest{
		NodeId: s.ID,
		Peers:  toProtoPeers(infos),
	})
	if err != nil {
		log.Printf("[%s] Ping to %s failed: %v", s.ID, peerAddr, err)
		s.closeConn(peerAddr)
		if s.peers.IncrementStaleCount(peerID) {
			s.notifyChange()
		}
		return
	}

	responsePeers := fromProtoPeers(resp.Peers)
	peerMarkedDead := s.peers.ProcessResponse(responsePeers)

	if peerMarkedDead {
		s.notifyChange()
	}

	log.Printf("[%s] Gossiped with %s, now know %d peers (%d alive)",
		s.ID, peerID, len(s.peers.GetAll()), len(s.peers.GetAlive()))
}

func (s *Server) pickRandomPeer() (string, string) {
	peers := s.peers.GetAliveForGossip()
	candidates := make([]Peer, 0, len(peers))

	for _, p := range peers {
		if p.ID != s.ID && p.Addr != s.Addr {
			candidates = append(candidates, p)
		}
	}

	if len(candidates) == 0 {
		return "", ""
	}

	chosen := candidates[rand.Intn(len(candidates))]
	return chosen.ID, chosen.Addr
}

func (s *Server) getConn(addr string) (*grpc.ClientConn, error) {
	s.connMu.RLock()
	conn, ok := s.conns[addr]
	s.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	s.connMu.Lock()
	defer s.connMu.Unlock()
	if conn, ok := s.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.conns[addr] = conn
	return conn, nil
}

func (s *Server) closeConn(addr string) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if conn, ok := s.conns[addr]; ok {
		conn.Close()
		delete(s.conns, addr)
	}
}

func toProtoPeers(infos []PeerInfo) []*pb.PeerInfo {
	result := make([]*pb.PeerInfo, len(infos))
	for i, info := range infos {
		result[i] = &pb.PeerInfo{
			Id:        info.ID,
			Addr:      info.Addr,
			Heartbeat: info.Heartbeat,
			Services:  info.Services,
		}
	}
	return result
}

func fromProtoPeers(peers []*pb.PeerInfo) []PeerInfo {
	result := make([]PeerInfo, len(peers))
	for i, p := range peers {
		result[i] = PeerInfo{
			ID:        p.Id,
			Addr:      p.Addr,
			Heartbeat: p.Heartbeat,
			Services:  p.Services,
		}
	}
	return result
}
