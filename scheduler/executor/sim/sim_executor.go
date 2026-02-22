package sim

import (
	"math/rand"
	"time"

	"github.com/chorus/scheduler/job"
)

type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(j *job.Job, onToken func(string)) error {
	jitter := time.Duration(rand.Intn(50)) * time.Millisecond
	time.Sleep(j.Duration + jitter)
	return nil
}
