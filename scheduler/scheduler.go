package scheduler

import (
	"container/heap"
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
	// 2.2: s.reclaim()
	// 2.3: s.admit()
}

// CurrentTick returns the current tick count.
func (s *Scheduler) CurrentTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

// Stats returns a snapshot of the scheduler's current state.
func (s *Scheduler) Stats() (pending, running, availableCapacity int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending.Len(), len(s.running), s.pool.Available()
}
