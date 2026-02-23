package worker

import (
	"fmt"

	"github.com/chorus/scheduler/executor/llm"
	"github.com/chorus/scheduler/executor/sim"
	"github.com/chorus/scheduler/job"
)

// Executor knows how to run a specific type of job.
type Executor interface {
	Execute(j *job.Job, onToken func(string)) error
}

// Worker represents a goroutine-backed executor with a fixed capacity budget.
type Worker struct {
	ID        string
	Capacity  int                // Total capacity units this worker can handle
	Used      int                // Currently consumed capacity units
	Jobs      []string           // IDs of jobs currently assigned to this worker
	Executors map[string]Executor // executor registry
}

// RegisterExecutor makes an executor available to a worker
func (w *Worker) RegisterExecutor(jobType string, exec Executor) {
	if w.Executors == nil {
		w.Executors = make(map[string]Executor)
	}
	w.Executors[jobType] = exec
}

// Execute runs a given job and also passes the completions channel so that the scheduler can be made aware once a job is done.
func (w *Worker) Execute(j *job.Job, completions chan string) {
	key := "sim"
	if j.JobType != nil && j.JobType.GetPromptContinuation() != nil {
		key = "prompt_continuation"
	}
	executor, ok := w.Executors[key]
	if !ok {
		if j.OutputCh != nil {
			close(j.OutputCh)
		}
		completions <- j.ID
		return
	}

	go func() {
		defer func() {
			if j.OutputCh != nil {
				close(j.OutputCh)
			}
			completions <- j.ID
		}()

		executor.Execute(j, func(token string) {
			if j.OutputCh != nil {
				j.OutputCh <- token
			}
		})
	}()
}

const NumWorkers = 4

// WorkerPool manages a fixed set of workers and their capacity bookkeeping.
type WorkerPool struct {
	Workers []*Worker
}

// NewWorkerPool creates a pool with NumWorkers workers, each with the given capacity.
func NewWorkerPool(capacityPerWorker int) *WorkerPool {
	workers := make([]*Worker, NumWorkers)
	for i := range workers {
		w := &Worker{
			ID:       fmt.Sprintf("worker-%d", i),
			Capacity: capacityPerWorker,
		}
		w.RegisterExecutor("sim", sim.NewExecutor())
		w.RegisterExecutor("prompt_continuation", llm.NewLLMExecutor(
			llm.NewClient("http://localhost:11434"), "llama3.2",
		))
		workers[i] = w
	}
	return &WorkerPool{Workers: workers}
}

// GetWorker returns a worker via its ID, linear scan for now
func (wp *WorkerPool) GetWorker(id string) *Worker {
	for _, w := range wp.Workers {
		if w.ID == id {
			return w
		}
	}
	return nil
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
