package backend

import (
	"context"
	"fmt"
	"strings"

	"github.com/chorus/node"
	pb "github.com/chorus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerBackend provides a way to fetch data from peers
type PeerBackend struct {
	node          *node.Node
	selfChunkAddr string
}

// NewPeerBackend instantiates a peer backend that uses the local chorus node's
// KV for discovery and dials peers' kvservers for actual chunk fetches.
func NewPeerBackend(node *node.Node, selfChunkAddr string) (*PeerBackend, error) {
	return &PeerBackend{
		node:          node,
		selfChunkAddr: selfChunkAddr,
	}, nil
}

// Close cleans up the remote connection
func (p *PeerBackend) Close() error {
	return nil
}

const maxPeersPerFetch = 3

// Fetch looks up which peers hold `hash` via the cluster KV, dials up to
// maxPeersPerFetch of them in parallel, and returns the first successful response.
func (p *PeerBackend) Fetch(hash []byte) ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	discValue, err := p.clusterGetMembership(ctx, "disc___"+string(hash))
	if err != nil {
		return nil, err
	}

	peers := make([]string, 0)
	for _, a := range parseAddrs(discValue) {
		if a != p.selfChunkAddr {
			peers = append(peers, a)
		}
	}
	if len(peers) == 0 {
		return nil, ErrNotFound
	}
	if len(peers) > maxPeersPerFetch {
		peers = peers[:maxPeersPerFetch]
	}

	type result struct {
		bytes []byte
		err   error
	}
	ch := make(chan result, len(peers))
	for _, addr := range peers {
		go func(a string) {
			b, err := p.peerDial(ctx, a, hash)
			ch <- result{b, err}
		}(addr)
	}

	for remaining := len(peers); remaining > 0; remaining-- {
		r := <-ch
		if r.err == nil && r.bytes != nil {
			return r.bytes, nil
		}
	}
	return nil, ErrNotFound
}

func (p *PeerBackend) peerDial(ctx context.Context, addr string, hash []byte) ([]byte, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := pb.NewKVServiceClient(conn).Get(ctx, &pb.GetRequest{Key: string(hash)})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, ErrNotFound
	}
	return resp.Value, nil
}

func (p *PeerBackend) Stat(hash []byte) (Info, error) {
	res, err := p.Fetch(hash)
	if err != nil {
		return Info{}, err
	}
	return Info{Size: int64(len(res))}, nil
}

func (p *PeerBackend) Has(hash []byte) bool {
	_, err := p.Fetch(hash)
	return err == nil
}

// Announce records in the cluster KV that this worker now holds `hash`.
// Reads disc:<hash> (following any ring redirect), appends self if not already
// present, writes it back.
func (p *PeerBackend) Announce(hash []byte) error {
	ctx := context.Background()
	key := "disc___" + string(hash)

	current, err := p.clusterGetMembership(ctx, key)
	if err != nil {
		return fmt.Errorf("announce: lookup: %w", err)
	}

	addrs := parseAddrs(current)
	for _, a := range addrs {
		if a == p.selfChunkAddr {
			return nil // already announced
		}
	}
	addrs = append(addrs, p.selfChunkAddr)

	if err := p.clusterPut(ctx, key, []byte(strings.Join(addrs, ","))); err != nil {
		return fmt.Errorf("announce: put: %w", err)
	}
	return nil
}

// clusterGetMembership fetches a value from the local node's KV, following a ring
// redirect if the local node isn't the owner.
func (p *PeerBackend) clusterGetMembership(ctx context.Context, key string) ([]byte, error) {
	resp, err := p.node.KV.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	if resp.Found {
		return resp.Value, nil
	}
	if resp.OwnerAddr == "" {
		return nil, nil // genuinely absent
	}
	return p.remoteGet(ctx, resp.OwnerAddr, key)
}

// clusterPut writes a value to the local node's KV, following a ring redirect.
func (p *PeerBackend) clusterPut(ctx context.Context, key string, value []byte) error {
	resp, err := p.node.KV.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if resp.Stored {
		return nil
	}
	if resp.OwnerAddr == "" {
		return fmt.Errorf("put rejected with no redirect")
	}
	return p.remotePut(ctx, resp.OwnerAddr, key, value)
}

func (p *PeerBackend) remoteGet(ctx context.Context, addr, key string) ([]byte, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := pb.NewKVServiceClient(conn).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return resp.Value, nil
}

func (p *PeerBackend) remotePut(ctx context.Context, addr, key string, value []byte) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := pb.NewKVServiceClient(conn).Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if !resp.Stored {
		return fmt.Errorf("remote put rejected")
	}
	return nil
}

func parseAddrs(b []byte) []string {
	if len(b) == 0 {
		return nil
	}
	parts := strings.Split(string(b), ",")
	out := make([]string, 0, len(parts))
	for _, s := range parts {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

var _ Backend = (*PeerBackend)(nil)
