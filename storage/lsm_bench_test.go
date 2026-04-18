package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func setupBenchLSM(b *testing.B) *LSM {
	b.Helper()
	dir, err := os.MkdirTemp("", "lsm-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	lsm, err := NewLSM(dir)
	if err != nil {
		b.Fatalf("failed to create LSM: %v", err)
	}
	return lsm
}

func BenchmarkLSMInsert(b *testing.B) {
	lsm := setupBenchLSM(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%08d", i)
		val := fmt.Sprintf("val:%08d", i)
		if err := lsm.Insert([]byte(key), []byte(val)); err != nil {
			b.Fatalf("insert failed: %v", err)
		}
	}
}

// concurrentLoad inserts n keys using numWorkers goroutines to avoid
// sequential 10ms group commit waits per key during setup.
func concurrentLoad(b *testing.B, lsm *LSM, n int) {
	b.Helper()
	numWorkers := 100
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := start; i < n; i += numWorkers {
				key := fmt.Sprintf("key:%08d", i)
				val := fmt.Sprintf("val:%08d", i)
				lsm.Insert([]byte(key), []byte(val))
			}
		}(w)
	}
	wg.Wait()
}

func BenchmarkLSMInsert_Concurrent(b *testing.B) {
	for _, numWorkers := range []int{10, 100, 500, 1000, 10000} {
		b.Run(fmt.Sprintf("workers=%d", numWorkers), func(b *testing.B) {
			lsm := setupBenchLSM(b)
			perWorker := b.N / numWorkers
			if perWorker == 0 {
				perWorker = 1
			}

			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						key := fmt.Sprintf("key:%04d:%08d", id, i)
						val := fmt.Sprintf("val:%04d:%08d", id, i)
						lsm.Insert([]byte(key), []byte(val))
					}
				}(w)
			}
			wg.Wait()
		})
	}
}

func BenchmarkLSMGet_Memtable(b *testing.B) {
	lsm := setupBenchLSM(b)
	concurrentLoad(b, lsm, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%08d", i%10000)
		if _, err := lsm.Get([]byte(key)); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkLSMGet_SSTable(b *testing.B) {
	lsm := setupBenchLSM(b)

	// pre-load and flush to SSTable
	concurrentLoad(b, lsm, 10000)
	if err := lsm.Flush(); err != nil {
		b.Fatalf("flush failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%08d", i%10000)
		if _, err := lsm.Get([]byte(key)); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}
