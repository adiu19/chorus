package backend

import (
	"context"
	"fmt"
	"time"

	pb "github.com/chorus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RemoteKVServerBackend exposes a remote KV server as a backend, with an optional artifical latency
type RemoteKVServerBackend struct {
	latency int64
	conn    *grpc.ClientConn
	client  pb.KVServiceClient
}

// NewRemoteKVServerBackend instantiates a new backend, with an option to specify artificial latency
// if artificialLatencyInMS = 0, no latency is injected
func NewRemoteKVServerBackend(host string, port int, artificialLatencyInMS int64) (*RemoteKVServerBackend, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &RemoteKVServerBackend{
		latency: artificialLatencyInMS,
		conn:    conn,
		client:  pb.NewKVServiceClient(conn),
	}, nil
}

// Close cleans up the remote connection
func (sb *RemoteKVServerBackend) Close() error {
	return sb.conn.Close()
}

// Fetch returns the data bytes against an input hash
func (sb *RemoteKVServerBackend) Fetch(hash []byte) ([]byte, error) {
	if sb.latency > 0 {
		time.Sleep(time.Millisecond * time.Duration(sb.latency))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := sb.client.Get(ctx, &pb.GetRequest{Key: string(hash)})
	if err != nil {
		return nil, err
	}
	if !resp.Found || resp.Value == nil {
		return nil, ErrNotFound
	}
	return resp.Value, nil
}

func (sb *RemoteKVServerBackend) Stat(hash []byte) (Info, error) {
	res, err := sb.Fetch(hash)
	if err != nil {
		return Info{}, err
	}
	return Info{Size: int64(len(res))}, nil
}

func (sb *RemoteKVServerBackend) Has(hash []byte) bool {
	_, err := sb.Fetch(hash)
	if err != nil {
		return false
	}

	return true

}

var _ Backend = (*RemoteKVServerBackend)(nil)
