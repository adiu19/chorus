package scheduler

import "container/heap"

// PriorityQueue implements heap.Interface for Jobs.
// Lower Priority value = higher scheduling priority (popped first).
type PriorityQueue []*Job

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds a job to the queue. Called by heap.Push — do not call directly.
func (pq *PriorityQueue) Push(x any) {
	*pq = append(*pq, x.(*Job))
}

// Pop removes and returns the highest-priority job. Called by heap.Pop — do not call directly.
func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	job := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[:n-1]
	return job
}

// PushJob adds a job to the priority queue.
func PushJob(pq *PriorityQueue, job *Job) {
	heap.Push(pq, job)
}

// PopJob removes and returns the highest-priority (lowest Priority value) job.
func PopJob(pq *PriorityQueue) *Job {
	return heap.Pop(pq).(*Job)
}
