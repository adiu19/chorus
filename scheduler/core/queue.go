package core

import (
	"container/heap"

	"github.com/chorus/scheduler/job"
)

// PriorityQueue implements heap.Interface for Jobs.
// Lower Priority value = higher scheduling priority (popped first).
type PriorityQueue []*job.Job

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds a job to the queue. Called by heap.Push — do not call directly.
func (pq *PriorityQueue) Push(x any) {
	*pq = append(*pq, x.(*job.Job))
}

// Pop removes and returns the highest-priority job. Called by heap.Pop — do not call directly.
func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	j := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[:n-1]
	return j
}

// PushJob adds a job to the priority queue.
func PushJob(pq *PriorityQueue, j *job.Job) {
	heap.Push(pq, j)
}

// PopJob removes and returns the highest-priority (lowest Priority value) job.
func PopJob(pq *PriorityQueue) *job.Job {
	return heap.Pop(pq).(*job.Job)
}
