package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"log"
	"math/rand"
	"os"
	"path/filepath"
)

func main() {
	outDir := flag.String("dir", "/dev/shm/cas", "where to write blob files")
	blobCount := flag.Int("blobs", 1000, "")
	blobSize := flag.Int("size", 4096, "")
	seed := flag.Int64("seed", 42, "")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("mkdir %s: %v", *outDir, err)
	}

	rng := rand.New(rand.NewSource(*seed))
	for i := 0; i < *blobCount; i++ {
		buf := make([]byte, *blobSize)
		rng.Read(buf)
		h := sha256.Sum256(buf)
		path := filepath.Join(*outDir, hex.EncodeToString(h[:]))
		if err := os.WriteFile(path, buf, 0644); err != nil {
			log.Fatalf("write %s: %v", path, err)
		}
	}
	log.Printf("wrote %d blobs of %d bytes each to %s", *blobCount, *blobSize, *outDir)
}
