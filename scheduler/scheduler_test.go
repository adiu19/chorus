package scheduler

import (
	"testing"
	"time"
)

func TestScheduler_AdmitByPriority(t *testing.T) {
	sched := New(Config{CapacityPerWorker: 10, TickInterval: time.Second})

	sched.Submit(&Job{ID: "low", Priority: 10, Cost: 5})
	sched.Submit(&Job{ID: "high", Priority: 1, Cost: 5})
	sched.Submit(&Job{ID: "mid", Priority: 5, Cost: 5})

	sched.Tick()

	pending, running, _ := sched.Stats()
	if running != 3 {
		t.Errorf("expected 3 running (plenty of capacity), got %d running, %d pending", running, pending)
	}
}

func TestScheduler_SkipJobsThatDontFit(t *testing.T) {
	// 4 workers * 5 capacity = 20 total
	sched := New(Config{CapacityPerWorker: 5, TickInterval: time.Second})

	// These three cost 8 each = 24 total. Only two can fit (each on separate workers).
	// The third (lowest priority) should be skipped.
	sched.Submit(&Job{ID: "a", Priority: 1, Cost: 5})
	sched.Submit(&Job{ID: "b", Priority: 2, Cost: 5})
	sched.Submit(&Job{ID: "c", Priority: 3, Cost: 5})
	sched.Submit(&Job{ID: "d", Priority: 4, Cost: 5})
	sched.Submit(&Job{ID: "e", Priority: 5, Cost: 5})

	sched.Tick()

	pending, running, avail := sched.Stats()
	// 4 workers can take 4 jobs (cost 5 each), 1 remains pending
	if running != 4 {
		t.Errorf("expected 4 running, got %d", running)
	}
	if pending != 1 {
		t.Errorf("expected 1 pending, got %d", pending)
	}
	if avail != 0 {
		t.Errorf("expected 0 available capacity, got %d", avail)
	}
}

func TestScheduler_SmallJobAdmittedWhenLargeDoesntFit(t *testing.T) {
	// 4 workers * 4 capacity = 16 total
	sched := New(Config{CapacityPerWorker: 4, TickInterval: time.Second})

	// Fill 3 workers to capacity
	sched.Submit(&Job{ID: "fill-1", Priority: 0, Cost: 4})
	sched.Submit(&Job{ID: "fill-2", Priority: 0, Cost: 4})
	sched.Submit(&Job{ID: "fill-3", Priority: 0, Cost: 4})
	sched.Tick()

	// One worker left with 4 free. Submit a large job (cost 5, won't fit anywhere)
	// and a small job (cost 2, fits). Small job should be admitted despite lower priority.
	sched.Submit(&Job{ID: "big", Priority: 1, Cost: 5})
	sched.Submit(&Job{ID: "small", Priority: 2, Cost: 2})
	sched.Tick()

	pending, running, _ := sched.Stats()
	if running != 4 { // 3 fills + small
		t.Errorf("expected 4 running, got %d", running)
	}
	if pending != 1 { // big is still pending
		t.Errorf("expected 1 pending (big job), got %d", pending)
	}
}

func TestScheduler_ReclaimThenAdmit(t *testing.T) {
	sched := New(Config{CapacityPerWorker: 5, TickInterval: time.Second})

	sched.Submit(&Job{ID: "first", Priority: 1, Cost: 5})
	sched.Tick() // first -> running

	// Mark it completed (simulating what a worker goroutine would do in Milestone 3)
	sched.mu.Lock()
	sched.running["first"].Status = Completed
	sched.mu.Unlock()

	// Submit another job that needs that capacity
	sched.Submit(&Job{ID: "second", Priority: 1, Cost: 5})
	sched.Tick() // reclaim frees worker, admit picks up second

	pending, running, _ := sched.Stats()
	if running != 1 {
		t.Errorf("expected 1 running (second), got %d", running)
	}
	if pending != 0 {
		t.Errorf("expected 0 pending, got %d", pending)
	}
}
