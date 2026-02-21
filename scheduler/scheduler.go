package scheduler

import (
	"container/heap"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Config holds the scheduler's tunable parameters.
type Config struct {
	CapacityPerWorker int
	TickInterval      time.Duration
	MaxPendingJobs    int // 0 means unlimited
}

// Scheduler is the central coordinator that matches pending jobs to workers.
// It operates in discrete ticks: each tick reclaims completed work and admits new jobs.
type Scheduler struct {
	mu          sync.Mutex
	config      Config
	tick        uint64 // monotonically increasing tick counter
	pending     *PriorityQueue
	running     map[string]*Job // job ID -> job
	pool        *WorkerPool
	completions chan string   // worker goroutines send job IDs here when done
	stop        chan struct{} // signals the tick loop to shut down
	allJobs     sync.Map      // job ID -> *Job; lock-free registry for read-heavy status queries
}

// New creates a Scheduler with the given config.
// The scheduler is inert until Start() is called (Sched 2.4).
func New(cfg Config) *Scheduler {
	pq := &PriorityQueue{}
	heap.Init(pq)

	return &Scheduler{
		config:      cfg,
		pending:     pq,
		running:     make(map[string]*Job),
		pool:        NewWorkerPool(cfg.CapacityPerWorker),
		completions: make(chan string, 64),
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
// Returns an error if the pending queue is at capacity.
func (s *Scheduler) Submit(job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.config.MaxPendingJobs > 0 && s.pending.Len() >= s.config.MaxPendingJobs {
		job.Status = Rejected
		job.CreatedAt = time.Now()
		s.allJobs.Store(job.ID, job)
		return fmt.Errorf("pending queue full (%d/%d)", s.pending.Len(), s.config.MaxPendingJobs)
	}

	job.Status = Pending
	job.CreatedAt = time.Now()
	s.allJobs.Store(job.ID, job)
	PushJob(s.pending, job)
	return nil
}

// Tick runs one scheduler cycle. The tick is the core event loop primitive:
//
// All state mutations happen inside the tick, under the lock, sequentially.
// Worker goroutines never touch scheduler state directly — they only send
// completion signals onto a channel (the "mailbox"). The tick loop is the
// sole decision-maker.
//
// This is what makes it an event loop rather than a concurrent free-for-all.
// The channel is the event source, the tick is the processing loop, and the
// separation between "events arrive async" and "events are processed
// synchronously in batches" is the core pattern.
//
//	No step starts until the previous one finishes. That's the invariant.
//	Even though the world outside the tick is async (goroutines completing, gRPC requests arriving),
//	inside the tick it's a synchronous pipeline.
//	This is what makes reasoning about state tractable
//		— at any point in the tick, you know exactly what's happened before.
//
// Each tick runs three phases in strict order:
//  1. drain()   — snapshot the completions channel, mark finished jobs
//  2. reclaim() — free capacity from completed jobs
//  3. admit()   — assign pending jobs to freed workers
//
// No phase starts until the previous one finishes.
func (s *Scheduler) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tick++
	s.drain()
	s.reclaim()
	s.admit()
}

// CurrentTick returns the current tick count.
func (s *Scheduler) CurrentTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

// drain reads completed job IDs from the completions channel and marks them
// as Completed in the running map. Uses a snapshot of the channel length to
// guarantee bounded processing — any completions arriving during drain are
// left for the next tick. This prevents an unbounded drain loop in the
// (theoretical) case where completions arrive as fast as we read them.
// Must be called with s.mu held.
func (s *Scheduler) drain() {
	n := len(s.completions) // snapshot the length
	for i := 0; i < n; i++ {
		id := <-s.completions
		if job, ok := s.running[id]; ok {
			job.Status = Completed
		}
	}
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
			s.execute(job)
		} else {
			skipped = append(skipped, job)
		}
	}
	// Re-queue jobs that didn't fit
	for _, job := range skipped {
		PushJob(s.pending, job)
	}
}

// execute spawns a goroutine that simulates job work by sleeping for the
// job's duration (plus a small random jitter), then signals completion.
// Must be called with s.mu held (reads job fields, but the goroutine itself
// only sends on the completions channel — no lock needed inside).
func (s *Scheduler) execute(job *Job) {
	jitter := time.Duration(rand.Intn(50)) * time.Millisecond
	go func() {
		time.Sleep(job.Duration + jitter)
		s.completions <- job.ID
	}()
}

// GetJob returns a job by ID from the registry. Lock-free; may return slightly stale status.
func (s *Scheduler) GetJob(id string) (*Job, bool) {
	v, ok := s.allJobs.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*Job), true
}

// GetAllJobs returns all jobs in the registry. Lock-free; may return slightly stale status.
func (s *Scheduler) GetAllJobs() []*Job {
	var jobs []*Job
	s.allJobs.Range(func(_, v any) bool {
		jobs = append(jobs, v.(*Job))
		return true
	})
	return jobs
}

// Stats returns a snapshot of the scheduler's current state.
func (s *Scheduler) Stats() (pending, running, availableCapacity int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending.Len(), len(s.running), s.pool.Available()
}
