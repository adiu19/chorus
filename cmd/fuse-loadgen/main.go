package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func main() {
	mount := flag.String("mount", "/tmp/cas", "directory containing blob files (FUSE mount or tmpfs)")
	concurrency := flag.Int("concurrency", 64, "concurrent reader goroutines")
	opsPerWorker := flag.Int("ops", 1000, "operations per worker")
	blobCount := flag.Int("blobs", 1000, "total blobs available in the mount")
	blobSize := flag.Int("size", 4096, "blob size in bytes (must match populate)")
	seed := flag.Int64("seed", 42, "seed used to populate the mount")
	out := flag.String("out", "", "append-mode JSONL output path (stdout if empty)")
	label := flag.String("label", "fuse", "label written into each JSONL row (e.g., fuse, tmpfs)")
	flag.Parse()

	hashes := deriveHashes(*seed, *blobCount, *blobSize)

	openLat := make([][]time.Duration, *concurrency)
	readLat := make([][]time.Duration, *concurrency)
	openErr := make([]int, *concurrency)
	readErr := make([]int, *concurrency)

	for w := 0; w < *concurrency; w++ {
		openLat[w] = make([]time.Duration, 0, *opsPerWorker)
		readLat[w] = make([]time.Duration, 0, *opsPerWorker)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(wid) + *seed))
			buf := make([]byte, *blobSize)
			for i := 0; i < *opsPerWorker; i++ {
				h := hashes[rng.Intn(len(hashes))]
				path := filepath.Join(*mount, hex.EncodeToString(h[:]))

				t0 := time.Now()
				f, err := os.Open(path)
				openLat[wid] = append(openLat[wid], time.Since(t0))
				if err != nil {
					openErr[wid]++
					continue
				}

				t1 := time.Now()
				n, err := io.ReadFull(f, buf)
				readLat[wid] = append(readLat[wid], time.Since(t1))
				f.Close()
				if err != nil || n != *blobSize {
					readErr[wid]++
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	allOpen := flatten(openLat)
	allRead := flatten(readLat)
	totalOpenErr := sumInts(openErr)
	totalReadErr := sumInts(readErr)

	emit(*out, result{
		Label:       *label,
		Op:          "open",
		Concurrency: *concurrency,
		Ops:         len(allOpen),
		Errors:      totalOpenErr,
		Percentiles: percentiles(allOpen),
		ElapsedNs:   elapsed.Nanoseconds(),
	})
	emit(*out, result{
		Label:       *label,
		Op:          "read",
		Concurrency: *concurrency,
		Ops:         len(allRead),
		Errors:      totalReadErr,
		Percentiles: percentiles(allRead),
		ElapsedNs:   elapsed.Nanoseconds(),
	})
}

type result struct {
	Label       string         `json:"label"`
	Op          string         `json:"op"`
	Concurrency int            `json:"concurrency"`
	Ops         int            `json:"ops"`
	Errors      int            `json:"errors"`
	Percentiles map[string]int64 `json:"percentiles_ns"`
	ElapsedNs   int64          `json:"elapsed_ns"`
}

func emit(path string, r result) {
	b, _ := json.Marshal(r)
	if path == "" {
		fmt.Println(string(b))
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	f.Write(b)
	f.Write([]byte("\n"))
}

func deriveHashes(seed int64, count, size int) [][32]byte {
	rng := rand.New(rand.NewSource(seed))
	hashes := make([][32]byte, count)
	buf := make([]byte, size)
	for i := 0; i < count; i++ {
		rng.Read(buf)
		hashes[i] = sha256.Sum256(buf)
	}
	return hashes
}

func flatten(per [][]time.Duration) []time.Duration {
	total := 0
	for _, s := range per {
		total += len(s)
	}
	out := make([]time.Duration, 0, total)
	for _, s := range per {
		out = append(out, s...)
	}
	return out
}

func sumInts(s []int) int {
	t := 0
	for _, v := range s {
		t += v
	}
	return t
}

func percentiles(d []time.Duration) map[string]int64 {
	if len(d) == 0 {
		return map[string]int64{}
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	pick := func(p float64) int64 {
		idx := int(float64(len(d)-1) * p)
		return d[idx].Nanoseconds()
	}
	return map[string]int64{
		"p50":   pick(0.50),
		"p95":   pick(0.95),
		"p99":   pick(0.99),
		"p99_9": pick(0.999),
		"max":   d[len(d)-1].Nanoseconds(),
	}
}
