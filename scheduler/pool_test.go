package scheduler

import "testing"

func TestWorkerPool_AdmitAndRelease(t *testing.T) {
	pool := NewWorkerPool(10)

	// Should have 4 workers with 40 total capacity
	if avail := pool.Available(); avail != 40 {
		t.Fatalf("expected 40 available, got %d", avail)
	}

	// All workers have 10 available — first admit goes to worker-0 (tie-break by scan order)
	wid, ok := pool.Admit(7)
	if !ok {
		t.Fatal("expected admit, got rejection")
	}
	if avail := pool.Available(); avail != 33 {
		t.Errorf("expected 33 available, got %d", avail)
	}

	// Second cost-7 job: worker-0 has 3 left (too small), workers 1-3 have 10.
	// All three tie at 10 — goes to worker-1 by scan order.
	wid, ok = pool.Admit(7)
	if !ok {
		t.Fatal("expected admit, got rejection")
	}

	// Release the first job
	pool.Release("worker-0", 7)
	if avail := pool.Available(); avail != 33 {
		t.Errorf("expected 33 after release, got %d", avail)
	}

	// Now worker-0 has 10, worker-1 has 3, workers 2-3 have 10.
	// Cost-3 job should go to worker-1 (best-fit: exactly 3 available).
	wid, ok = pool.Admit(3)
	if !ok {
		t.Fatal("expected admit, got rejection")
	}
	if wid != "worker-1" {
		t.Errorf("expected best-fit on worker-1 (3 avail for cost 3), got %s", wid)
	}
}

func TestWorkerPool_RejectWhenFull(t *testing.T) {
	pool := NewWorkerPool(5)

	// Fill every worker to capacity
	for i := 0; i < NumWorkers; i++ {
		_, ok := pool.Admit(5)
		if !ok {
			t.Fatalf("expected admit on worker-%d", i)
		}
	}

	if avail := pool.Available(); avail != 0 {
		t.Fatalf("expected 0 available, got %d", avail)
	}

	// Next admit should fail
	_, ok := pool.Admit(1)
	if ok {
		t.Error("expected rejection when all workers are full")
	}
}

func TestWorkerPool_CostLargerThanAnyWorker(t *testing.T) {
	pool := NewWorkerPool(10)

	// A job that costs more than any single worker can handle
	_, ok := pool.Admit(11)
	if ok {
		t.Error("expected rejection for cost exceeding worker capacity")
	}

	// Capacity should be unchanged
	if avail := pool.Available(); avail != 40 {
		t.Errorf("expected 40 available, got %d", avail)
	}
}
