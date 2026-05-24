package kvserver

import (
	"context"
	"log"

	pb "github.com/chorus/proto"
	"github.com/chorus/storage"
)

type Server struct {
	pb.UnimplementedKVServiceServer
	store storage.Store
}

func New(store storage.Store) *Server {
	return &Server{store: store}
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := s.store.Insert([]byte(req.Key), req.Value); err != nil {
		return nil, err
	}
	log.Printf("[kvserver] Put key=%s", req.Key)
	return &pb.PutResponse{Stored: true}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := s.store.Get([]byte(req.Key))
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{
		Found: value != nil,
		Value: value,
	}, nil
}

func (s *Server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if err := s.store.Insert([]byte(req.Key), req.Value); err != nil {
		return nil, err
	}
	return &pb.ReplicateResponse{Stored: true}, nil
}

func (s *Server) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	return &pb.FetchResponse{}, nil
}
