package llm

import "github.com/chorus/scheduler/job"

// Executor is a struct that can be thought of as entity meant to execute an LLM-style job
type Executor struct {
	client *Client
	model  string
}

// NewLLMExecutor instantiates an LLM executor
func NewLLMExecutor(client *Client, model string) *Executor {
	return &Executor{client: client, model: model}
}

// Execute initiates a streaming token job
func (e *Executor) Execute(j *job.Job, onToken func(string)) error {
	return e.client.ChatStream(&ChatRequest{
		Model:    e.model,
		Messages: []Message{{Role: "user", Content: j.JobType.GetPromptContinuation().GetPrompt()}},
	}, onToken)
}
