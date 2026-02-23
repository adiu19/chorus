package core

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chorus/scheduler/job"
	"github.com/chorus/scheduler/worker"
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
	running     map[string]*job.Job // job ID -> job
	pool        *worker.WorkerPool
	completions chan string   // worker goroutines send job IDs here when done
	stop        chan struct{} // signals the tick loop to shut down
	allJobs     sync.Map     // job ID -> *Job; lock-free registry for read-heavy status queries
}

// New creates a Scheduler with the given config.
// The scheduler is inert until Start() is called (Sched 2.4).
func New(cfg Config) *Scheduler {
	pq := &PriorityQueue{}
	heap.Init(pq)

	return &Scheduler{
		config:      cfg,
		pending:     pq,
		running:     make(map[string]*job.Job),
		pool:        worker.NewWorkerPool(cfg.CapacityPerWorker),
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
func (s *Scheduler) Submit(j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.config.MaxPendingJobs > 0 && s.pending.Len() >= s.config.MaxPendingJobs {
		j.Status = job.Rejected
		j.CreatedAt = time.Now()
		s.allJobs.Store(j.ID, j)
		return fmt.Errorf("pending queue full (%d/%d)", s.pending.Len(), s.config.MaxPendingJobs)
	}

	j.Status = job.Pending
	j.CreatedAt = time.Now()
	s.allJobs.Store(j.ID, j)
	PushJob(s.pending, j)
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
		if j, ok := s.running[id]; ok {
			j.Status = job.Completed
		}
	}
}

// reclaim scans running jobs, frees capacity for completed ones, and removes them.
// Must be called with s.mu held.
func (s *Scheduler) reclaim() {
	for id, j := range s.running {
		if j.Status == job.Completed {
			s.pool.Release(j.WorkerID, j.ID, j.Cost)
			delete(s.running, id)
		}
	}
}

// admit drains the pending queue in priority order, assigning jobs to workers
// where capacity allows. Jobs that don't fit are re-queued for the next tick.
// Must be called with s.mu held.
func (s *Scheduler) admit() {
	// Drain the PQ into a slice so we can try each job and re-queue skipped ones.
	var skipped []*job.Job
	for s.pending.Len() > 0 {
		j := PopJob(s.pending)
		workerID, ok := s.pool.Admit(j.ID, j.Cost)
		if ok {
			j.Status = job.Running
			j.WorkerID = workerID
			j.StartedAt = time.Now()
			s.running[j.ID] = j
			s.execute(j)
		} else {
			skipped = append(skipped, j)
		}
	}
	// Re-queue jobs that didn't fit
	for _, j := range skipped {
		PushJob(s.pending, j)
	}
}

// execute fetches the worker that owns this job, and then dispatches the job to that worker
func (s *Scheduler) execute(j *job.Job) {
	w := s.pool.GetWorker(j.WorkerID)
	w.Execute(j, s.completions)
}

// GetJob returns a job by ID from the registry. Lock-free; may return slightly stale status.
func (s *Scheduler) GetJob(id string) (*job.Job, bool) {
	v, ok := s.allJobs.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*job.Job), true
}

// GetAllJobs returns all jobs in the registry. Lock-free; may return slightly stale status.
func (s *Scheduler) GetAllJobs() []*job.Job {
	var jobs []*job.Job
	s.allJobs.Range(func(_, v any) bool {
		jobs = append(jobs, v.(*job.Job))
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
