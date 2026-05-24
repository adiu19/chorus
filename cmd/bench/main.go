package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/chorus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "node address")
	rate := flag.Int("rate", 10000, "target requests per second")
	workers := flag.Int("workers", 10000, "number of worker goroutines")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	value := make([]byte, *valueSize)
	rand.Read(value)

	tokenCh := make(chan time.Time, *workers)
	doneCh := make(chan struct{})

	tokensPerTick := *rate / 1000
	if tokensPerTick < 1 {
		tokensPerTick = 1
	}

	var totalOps atomic.Int64
	var totalIntended atomic.Int64
	latencies := make([][]time.Duration, *workers)

	var wg sync.WaitGroup

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		latencies[w] = make([]time.Duration, 0, 1000)
		go func(id int) {
			defer wg.Done()
			i := 0
			for ts := range tokenCh {
				key := fmt.Sprintf("bench:%04d:%08d", id, i)
				_, err := client.Put(context.Background(), &pb.PutRequest{
					Key:   key,
					Value: value,
				})
				latency := time.Since(ts)

				if err != nil {
					continue
				}

				latencies[id] = append(latencies[id], latency)
				totalOps.Add(1)
				i++
			}
		}(w)
	}

	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		defer close(tokenCh)

		for {
			select {
			case <-doneCh:
				return
			case t := <-ticker.C:
				for i := 0; i < tokensPerTick; i++ {
					select {
					case tokenCh <- t:
						totalIntended.Add(1)
					case <-doneCh:
						return
					}
				}
			}
		}
	}()

	start := time.Now()
	time.Sleep(*duration)
	close(doneCh)
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, wl := range latencies {
		all = append(all, wl...)
	}

	ops := totalOps.Load()
	intended := totalIntended.Load()

	fmt.Println("--- benchmark results ---")
	fmt.Printf("duration:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("workers:     %d\n", *workers)
	fmt.Printf("target rate: %d/s\n", *rate)
	fmt.Printf("value size:  %d bytes\n", *valueSize)
	fmt.Printf("intended:    %d\n", intended)
	fmt.Printf("completed:   %d\n", ops)
	fmt.Printf("throughput:  %d ops/sec (target)\n", *rate)
	if len(all) > 0 {
		fmt.Printf("latency p50:  %v\n", percentile(all, 0.50))
		fmt.Printf("latency p99:  %v\n", percentile(all, 0.99))
		fmt.Printf("latency p999: %v\n", percentile(all, 0.999))
	}
}

func percentile(durations []time.Duration, p float64) time.Duration {
	n := len(durations)
	if n == 0 {
		return 0
	}

	buckets := make(map[time.Duration]int)
	for _, d := range durations {
		rounded := d.Round(100 * time.Microsecond)
		buckets[rounded]++
	}

	sorted := make([]time.Duration, 0, len(buckets))
	for d := range buckets {
		sorted = append(sorted, d)
	}
	sortDurations(sorted)

	target := int(float64(n) * p)
	cumulative := 0
	for _, d := range sorted {
		cumulative += buckets[d]
		if cumulative >= target {
			return d
		}
	}
	return sorted[len(sorted)-1]
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}
