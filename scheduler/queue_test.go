package scheduler

import (
	"container/heap"
	"testing"
)

func TestPriorityQueue_PopOrder(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	// Push jobs in random priority order
	jobs := []*Job{
		{ID: "low", Priority: 10},
		{ID: "high", Priority: 1},
		{ID: "mid", Priority: 5},
		{ID: "urgent", Priority: 0},
		{ID: "mid2", Priority: 5},
	}
	for _, j := range jobs {
		PushJob(pq, j)
	}

	if pq.Len() != 5 {
		t.Fatalf("expected len 5, got %d", pq.Len())
	}

	// Pop should return in ascending priority order (0, 1, 5, 5, 10)
	expected := []struct {
		id       string
		priority int
	}{
		{"urgent", 0},
		{"high", 1},
		// mid and mid2 both have priority 5 — order between them is heap-dependent
	}

	for _, want := range expected {
		got := PopJob(pq)
		if got.ID != want.id || got.Priority != want.priority {
			t.Errorf("expected {%s, %d}, got {%s, %d}", want.id, want.priority, got.ID, got.Priority)
		}
	}

	// Remaining two should both be priority 5
	j1 := PopJob(pq)
	j2 := PopJob(pq)
	if j1.Priority != 5 || j2.Priority != 5 {
		t.Errorf("expected both priority 5, got %d and %d", j1.Priority, j2.Priority)
	}

	// Last one should be priority 10
	last := PopJob(pq)
	if last.ID != "low" || last.Priority != 10 {
		t.Errorf("expected {low, 10}, got {%s, %d}", last.ID, last.Priority)
	}
}

func TestPriorityQueue_Empty(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	if pq.Len() != 0 {
		t.Fatalf("expected empty queue, got len %d", pq.Len())
	}

	// Push one, pop one, verify empty again
	PushJob(pq, &Job{ID: "only", Priority: 3})
	got := PopJob(pq)
	if got.ID != "only" {
		t.Errorf("expected 'only', got %s", got.ID)
	}
	if pq.Len() != 0 {
		t.Errorf("expected empty after pop, got len %d", pq.Len())
	}
}

func TestPriorityQueue_SingleElement(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	PushJob(pq, &Job{ID: "solo", Priority: 42})
	if pq.Len() != 1 {
		t.Fatalf("expected len 1, got %d", pq.Len())
	}

	got := PopJob(pq)
	if got.ID != "solo" || got.Priority != 42 {
		t.Errorf("expected {solo, 42}, got {%s, %d}", got.ID, got.Priority)
	}
}
