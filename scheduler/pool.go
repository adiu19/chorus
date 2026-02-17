package scheduler

import "fmt"

const NumWorkers = 4

// WorkerPool manages a fixed set of workers and their capacity bookkeeping.
type WorkerPool struct {
	Workers []*Worker
}

// NewWorkerPool creates a pool with NumWorkers workers, each with the given capacity.
func NewWorkerPool(capacityPerWorker int) *WorkerPool {
	workers := make([]*Worker, NumWorkers)
	for i := range workers {
		workers[i] = &Worker{
			ID:       fmt.Sprintf("worker-%d", i),
			Capacity: capacityPerWorker,
		}
	}
	return &WorkerPool{Workers: workers}
}

// Admit finds the worker with the tightest fit for the given cost (best-fit).
// Returns the worker ID and true if admitted, or empty string and false if no worker fits.
func (wp *WorkerPool) Admit(cost int) (string, bool) {
	var best *Worker
	for _, w := range wp.Workers {
		avail := w.Capacity - w.Used
		if avail >= cost {
			if best == nil || avail < best.Capacity-best.Used {
				best = w
			}
		}
	}
	if best == nil {
		return "", false
	}
	best.Used += cost
	return best.ID, true
}

// Release frees capacity on the specified worker.
func (wp *WorkerPool) Release(workerID string, cost int) {
	for _, w := range wp.Workers {
		if w.ID == workerID {
			w.Used -= cost
			return
		}
	}
}

// Available returns the total unused capacity across all workers.
func (wp *WorkerPool) Available() int {
	total := 0
	for _, w := range wp.Workers {
		total += w.Capacity - w.Used
	}
	return total
}
