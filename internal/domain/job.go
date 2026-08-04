// Package domain contains core business entities and invariants.
package domain

import (
	"fmt"
	"time"
)

// Job is the core queue entity.
type Job struct {
	ID             string
	Queue          Queue
	Payload        []byte
	State          JobState
	Attempts       int
	MaxAttempts    int
	RunAt          time.Time
	LeaseExpiresAt *time.Time
	WorkerID       *string
	IdempotencyKey *string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

// NewJob creates a pending job with validated defaults.
func NewJob(id string, queue Queue, payload []byte, maxAttempts int, runAt time.Time) (*Job, error) {
	if id == "" {
		return nil, fmt.Errorf("creating job: id is required")
	}
	if queue == "" {
		return nil, fmt.Errorf("creating job: queue is required")
	}
	if maxAttempts <= 0 {
		return nil, fmt.Errorf("creating job: max attempts must be > 0")
	}

	return &Job{
		ID:          id,
		Queue:       queue,
		Payload:     payload,
		State:       JobStatePending,
		MaxAttempts: maxAttempts,
		RunAt:       runAt,
		CreatedAt:   runAt,
		UpdatedAt:   runAt,
	}, nil
}

// Start marks a pending job as running for execution.
func (j *Job) Start() error {
	if j.State != JobStatePending {
		return fmt.Errorf("starting job from %s: %w", j.State, ErrInvalidTransition)
	}
	j.State = JobStateRunning
	j.Attempts++
	return nil
}

// CanRetry reports whether another attempt is available.
func (j Job) CanRetry() bool {
	return j.Attempts < j.MaxAttempts
}

// MarkFailed marks a running job as pending (retry) or dead (exhausted).
func (j *Job) MarkFailed(err error) error {
	if err == nil {
		return fmt.Errorf("marking job failed: error is required")
	}
	if j.State != JobStateRunning {
		return fmt.Errorf("marking failed from %s: %w", j.State, ErrInvalidTransition)
	}

	j.LastError = err.Error()
	if j.CanRetry() {
		j.State = JobStatePending
		return nil
	}

	j.State = JobStateDead
	return nil
}

// MarkSucceeded marks a running job as successfully completed.
func (j *Job) MarkSucceeded() error {
	if j.State != JobStateRunning {
		return fmt.Errorf("marking succeeded from %s: %w", j.State, ErrInvalidTransition)
	}
	j.State = JobStateSucceeded
	j.LastError = ""
	return nil
}

// RetryFromDead resets a dead job back to pending for manual replay.
func (j *Job) RetryFromDead() error {
	if j.State != JobStateDead {
		return fmt.Errorf("retrying from %s: %w", j.State, ErrInvalidTransition)
	}
	j.State = JobStatePending
	j.LastError = ""
	j.Attempts = 0
	return nil
}

// NextRunAt computes the next scheduled run based on attempts and retry policy.
func (j Job) NextRunAt(now time.Time, policy RetryPolicy, jitterFactor float64) (time.Time, error) {
	return ComputeNextRunAt(now, j.Attempts, policy, jitterFactor)
}
