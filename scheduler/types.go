package scheduler

import "time"

// JobStatus represents the lifecycle state of a job.
type JobStatus int

const (
	Pending   JobStatus = iota // Waiting in the queue to be scheduled
	Running                    // Assigned to a worker and executing
	Completed                  // Finished execution successfully
	Rejected                   // Denied admission (e.g. backpressure)
)

func (s JobStatus) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Completed:
		return "completed"
	case Rejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// Job represents a unit of work submitted to the scheduler.
type Job struct {
	ID        string
	Priority  int           // Lower value = higher priority
	Cost      int           // Capacity units this job consumes on a worker
	Duration  time.Duration // Simulated execution time
	Status    JobStatus
	WorkerID  string    // ID of the assigned worker (empty if not running)
	CreatedAt time.Time
	StartedAt time.Time // When the job transitioned to Running
}

// Worker represents a goroutine-backed executor with a fixed capacity budget.
type Worker struct {
	ID       string
	Capacity int      // Total capacity units this worker can handle
	Used     int      // Currently consumed capacity units
	Jobs     []string // IDs of jobs currently assigned to this worker
}
