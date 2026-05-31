package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"log"
	"math/rand"

	pb "github.com/chorus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "node address")
	blobCount := flag.Int("blobs", 1000, "")
	blobSize := flag.Int("size", 4096, "")
	seed := flag.Int64("seed", 42, "")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	rng := rand.New(rand.NewSource(*seed))
	for i := 0; i < *blobCount; i++ {
		buf := make([]byte, *blobSize)
		rng.Read(buf)
		h := sha256.Sum256(buf)
		_, err := client.Put(context.Background(), &pb.PutRequest{Key: string(h[:]), Value: buf})
		if err != nil {
			log.Fatalf("put %d: %v", i, err)
		}
	}
}
