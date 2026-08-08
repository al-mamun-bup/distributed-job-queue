// Package port defines use case input/output ports.
package port

import (
	"context"
	"time"

	"hopper/internal/domain"
)

type EnqueueInput struct {
	Queue          domain.Queue
	Payload        []byte
	MaxAttempts    int
	RunAt          time.Time
	IdempotencyKey *string
}

type FailInput struct {
	ID          string
	WorkerID    string
	RunAt       time.Time
	LastError   string
	MarkDead    bool
	CompletedAt *time.Time
}

// ListJobsInput filters a paginated job listing. Nil Queue/State mean
// "any". Limit <= 0 falls back to a repository-chosen default.
type ListJobsInput struct {
	Queue  *domain.Queue
	State  *domain.JobState
	Limit  int
	Offset int
}

// ListJobsOutput is a page of jobs plus the total matching row count, so
// callers can render pagination without a second round trip.
type ListJobsOutput struct {
	Jobs  []domain.Job
	Total int64
}

type QueueStats struct {
	Queue     string
	Pending   int64
	Running   int64
	Succeeded int64
	Dead      int64
	Total     int64
	UpdatedAt time.Time
}

// Notifier wakes a waiting worker when a new job may be available. It is a
// latency optimisation, not a delivery guarantee: a dropped or missed signal
// must never lose a job, since run_at <= now() polling is what actually
// guarantees delivery.
type Notifier interface {
	// Notifications delivers a best-effort wake-up signal per notification
	// received; it carries no payload, only "something changed, go check".
	Notifications() <-chan struct{}
	// Run holds the listener connection until ctx is done, reconnecting
	// with backoff on drop. It returns nil on ctx cancellation.
	Run(ctx context.Context) error
}

// JobRepository persists and claims jobs from durable storage.
type JobRepository interface {
	Enqueue(ctx context.Context, input EnqueueInput) (domain.Job, error)
	Claim(ctx context.Context, queue domain.Queue, batchSize int, leaseTTL time.Duration, workerID string) ([]domain.Job, error)
	Complete(ctx context.Context, id string, workerID string, completedAt time.Time) error
	Fail(ctx context.Context, input FailInput) error
	ExtendLease(ctx context.Context, id string, workerID string, leaseTTL time.Duration) error
	ReleaseLeases(ctx context.Context, workerID string, jobIDs []string) (int64, error)
	ReapExpired(ctx context.Context, batchSize int) ([]domain.Job, error)
	Get(ctx context.Context, id string) (domain.Job, error)
	List(ctx context.Context, input ListJobsInput) (ListJobsOutput, error)
	// Retry resurrects a dead job back to pending. Returns ErrInvalidTransition
	// if the job exists but isn't dead, ErrJobNotFound if it doesn't exist.
	Retry(ctx context.Context, id string) (domain.Job, error)
	Stats(ctx context.Context, queue domain.Queue) (QueueStats, error)
}
