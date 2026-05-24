package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/chorus/fuse"
	"github.com/chorus/fuse/backend"
	"github.com/chorus/storage"
	"github.com/chorus/storage/memstore"
)

func main() {
	mountPath := flag.String("mount", "/tmp/cas", "FUSE mount point")
	blobCount := flag.Int("blobs", 1000, "number of blobs to populate")
	blobSize := flag.Int("size", 4096, "blob size in bytes")
	seed := flag.Int64("seed", 42, "seed for deterministic random fill")
	debug := flag.Bool("debug", false, "enable FUSE debug logging")
	flag.Parse()

	store, err := memstore.NewMemStore()
	if err != nil {
		log.Fatalf("failed to initialize mem store: %v", err)
	}

	hashes := populate(store, *seed, *blobCount, *blobSize)
	log.Printf("populated %d blobs of %d bytes each", *blobCount, *blobSize)
	log.Printf("sample hashes:")
	for i := 0; i < 3 && i < len(hashes); i++ {
		log.Printf("  %s", hex.EncodeToString(hashes[i][:]))
	}

	srv, err := fuse.Mount(fuse.Config{
		MountPoint: *mountPath,
		Backend:    backend.NewStoreBackend(store),
		Debug:      *debug,
	})
	if err != nil {
		log.Fatalf("mount: %v", err)
	}
	log.Printf("mounted at %s — try `cat %s/<hash>`", *mountPath, *mountPath)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("unmounting...")
	if err := srv.Unmount(); err != nil {
		log.Printf("unmount error: %v", err)
	}
}

func populate(store storage.Store, seed int64, count, size int) [][32]byte {
	rng := rand.New(rand.NewSource(seed))
	hashes := make([][32]byte, count)
	for i := 0; i < count; i++ {
		buf := make([]byte, size)
		rng.Read(buf)
		h := sha256.Sum256(buf)
		hashes[i] = h
		if err := store.Insert(h[:], buf); err != nil {
			log.Fatalf("insert: %v", err)
		}
	}
	return hashes
}
