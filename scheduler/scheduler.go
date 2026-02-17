package scheduler

import (
	"container/heap"
	"log"
	"sync"
	"time"
)

// Config holds the scheduler's tunable parameters.
type Config struct {
	CapacityPerWorker int
	TickInterval      time.Duration
}

// Scheduler is the central coordinator that matches pending jobs to workers.
// It operates in discrete ticks: each tick reclaims completed work and admits new jobs.
type Scheduler struct {
	mu      sync.Mutex
	config  Config
	tick    uint64 // monotonically increasing tick counter
	pending *PriorityQueue
	running map[string]*Job // job ID -> job
	pool    *WorkerPool
	stop    chan struct{} // signals the tick loop to shut down
}

// New creates a Scheduler with the given config.
// The scheduler is inert until Start() is called (Sched 2.4).
func New(cfg Config) *Scheduler {
	pq := &PriorityQueue{}
	heap.Init(pq)

	return &Scheduler{
		config:  cfg,
		pending: pq,
		running: make(map[string]*Job),
		pool:    NewWorkerPool(cfg.CapacityPerWorker),
	}
}

// Start launches the tick loop in a background goroutine.
// Each tick fires on the interval defined in Config.TickInterval.
func (s *Scheduler) Start() {
	s.stop = make(chan struct{})
	ticker := time.NewTicker(s.config.TickInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.Tick()
				p, r, a := s.Stats()
				log.Printf("[scheduler] tick=%d pending=%d running=%d available_capacity=%d",
					s.CurrentTick(), p, r, a)
			case <-s.stop:
				return
			}
		}
	}()
}

// Stop shuts down the tick loop.
func (s *Scheduler) Stop() {
	close(s.stop)
}

// Submit adds a job to the pending queue. Thread-safe.
func (s *Scheduler) Submit(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job.Status = Pending
	job.CreatedAt = time.Now()
	PushJob(s.pending, job)
}

// Tick runs one scheduler cycle: reclaim completed work, then admit new jobs.
// Called by the tick loop (Sched 2.4) or directly in tests.
func (s *Scheduler) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tick++
	s.reclaim()
	s.admit()
}

// CurrentTick returns the current tick count.
func (s *Scheduler) CurrentTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

// reclaim scans running jobs, frees capacity for completed ones, and removes them.
// Must be called with s.mu held.
func (s *Scheduler) reclaim() {
	for id, job := range s.running {
		if job.Status == Completed {
			s.pool.Release(job.WorkerID, job.ID, job.Cost)
			delete(s.running, id)
		}
	}
}

// admit drains the pending queue in priority order, assigning jobs to workers
// where capacity allows. Jobs that don't fit are re-queued for the next tick.
// Must be called with s.mu held.
func (s *Scheduler) admit() {
	// Drain the PQ into a slice so we can try each job and re-queue skipped ones.
	var skipped []*Job
	for s.pending.Len() > 0 {
		job := PopJob(s.pending)
		workerID, ok := s.pool.Admit(job.ID, job.Cost)
		if ok {
			job.Status = Running
			job.WorkerID = workerID
			job.StartedAt = time.Now()
			s.running[job.ID] = job
		} else {
			skipped = append(skipped, job)
		}
	}
	// Re-queue jobs that didn't fit
	for _, job := range skipped {
		PushJob(s.pending, job)
	}
}

// Stats returns a snapshot of the scheduler's current state.
func (s *Scheduler) Stats() (pending, running, availableCapacity int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending.Len(), len(s.running), s.pool.Available()
}
