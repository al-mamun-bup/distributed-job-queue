package domain

import "time"

// JobState is the lifecycle state of a job.
type JobState string

const (
	// JobStatePending is ready for claiming when run_at <= now.
	JobStatePending JobState = "pending"
	// JobStateRunning is actively being processed by a worker.
	JobStateRunning JobState = "running"
	// JobStateSucceeded is terminal successful completion.
	JobStateSucceeded JobState = "succeeded"
	// JobStateDead is terminal failure after max attempts.
	JobStateDead JobState = "dead"
)

// Queue identifies a logical queue name.
type Queue string

// RetryPolicy controls exponential backoff delay and jitter.
type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    float64
}
