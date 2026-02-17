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
// Tracks the job ID on the worker. Returns the worker ID and true if admitted.
func (wp *WorkerPool) Admit(jobID string, cost int) (string, bool) {
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
	best.Jobs = append(best.Jobs, jobID)
	return best.ID, true
}

// Release frees capacity on the specified worker and removes the job from its job list.
func (wp *WorkerPool) Release(workerID string, jobID string, cost int) {
	for _, w := range wp.Workers {
		if w.ID == workerID {
			w.Used -= cost
			for i, id := range w.Jobs {
				if id == jobID {
					w.Jobs = append(w.Jobs[:i], w.Jobs[i+1:]...)
					break
				}
			}
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
