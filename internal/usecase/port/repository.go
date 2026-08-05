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

type QueueStats struct {
	Queue     string
	Pending   int64
	Running   int64
	Succeeded int64
	Dead      int64
	Total     int64
	UpdatedAt time.Time
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
	Stats(ctx context.Context, queue domain.Queue) (QueueStats, error)
}
