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

func TestScheduler_EndToEnd(t *testing.T) {
	// 4 workers, 10 capacity each. Tick every 100ms.
	sched := New(Config{CapacityPerWorker: 10, TickInterval: 100 * time.Millisecond})

	// Submit 5 jobs with varying costs and durations.
	// Total cost = 6+4+8+3+5 = 26, fits in 40 total capacity.
	// All should be admitted on tick 1.
	sched.Submit(&Job{ID: "j1", Priority: 1, Cost: 6, Duration: 150 * time.Millisecond})
	sched.Submit(&Job{ID: "j2", Priority: 2, Cost: 4, Duration: 80 * time.Millisecond})
	sched.Submit(&Job{ID: "j3", Priority: 3, Cost: 8, Duration: 200 * time.Millisecond})
	sched.Submit(&Job{ID: "j4", Priority: 4, Cost: 3, Duration: 50 * time.Millisecond})
	sched.Submit(&Job{ID: "j5", Priority: 5, Cost: 5, Duration: 120 * time.Millisecond})

	// Tick 1: all 5 admitted, goroutines start sleeping
	sched.Tick()
	p, r, _ := sched.Stats()
	if r != 5 || p != 0 {
		t.Fatalf("tick 1: expected 5 running 0 pending, got %d running %d pending", r, p)
	}

	// Wait long enough for the short jobs to complete (j4=50ms, j2=80ms + up to 50ms jitter)
	time.Sleep(200 * time.Millisecond)

	// Tick 2: drain completions, reclaim short jobs
	sched.Tick()
	p, r, a := sched.Stats()
	if r >= 5 {
		t.Errorf("tick 2: expected some jobs to have completed, still have %d running", r)
	}
	if a <= 0 {
		t.Errorf("tick 2: expected freed capacity, got %d", a)
	}

	// Wait for everything to finish (longest is j3=200ms + 50ms jitter)
	time.Sleep(300 * time.Millisecond)

	// Tick 3: all done
	sched.Tick()
	p, r, a = sched.Stats()
	if r != 0 {
		t.Errorf("tick 3: expected 0 running, got %d", r)
	}
	if p != 0 {
		t.Errorf("tick 3: expected 0 pending, got %d", p)
	}
	if a != 40 {
		t.Errorf("tick 3: expected full capacity (40), got %d", a)
	}
}

func TestScheduler_CompletionFreesCapacityForPending(t *testing.T) {
	// 4 workers, 5 capacity each = 20 total
	sched := New(Config{CapacityPerWorker: 5, TickInterval: 100 * time.Millisecond})

	// Fill all capacity: 4 jobs * cost 5 = 20
	sched.Submit(&Job{ID: "a", Priority: 1, Cost: 5, Duration: 100 * time.Millisecond})
	sched.Submit(&Job{ID: "b", Priority: 1, Cost: 5, Duration: 100 * time.Millisecond})
	sched.Submit(&Job{ID: "c", Priority: 1, Cost: 5, Duration: 100 * time.Millisecond})
	sched.Submit(&Job{ID: "d", Priority: 1, Cost: 5, Duration: 100 * time.Millisecond})

	// This one can't fit yet
	sched.Submit(&Job{ID: "waiting", Priority: 2, Cost: 5, Duration: 50 * time.Millisecond})

	// Tick 1: 4 admitted, "waiting" stays pending
	sched.Tick()
	p, r, _ := sched.Stats()
	if r != 4 || p != 1 {
		t.Fatalf("tick 1: expected 4 running 1 pending, got %d running %d pending", r, p)
	}

	// Wait for the first batch to complete
	time.Sleep(200 * time.Millisecond)

	// Tick 2: drain + reclaim frees capacity, admit picks up "waiting"
	sched.Tick()
	p, r, _ = sched.Stats()
	if r != 1 {
		t.Errorf("tick 2: expected 1 running (waiting), got %d", r)
	}
	if p != 0 {
		t.Errorf("tick 2: expected 0 pending, got %d", p)
	}
}
