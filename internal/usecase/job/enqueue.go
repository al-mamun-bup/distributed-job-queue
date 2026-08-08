// Package job contains job-related application use cases.
package job

import (
	"context"
	"fmt"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

// Enqueuer accepts new jobs into the queue.
type Enqueuer struct {
	repository port.JobRepository
}

func NewEnqueuer(repository port.JobRepository) *Enqueuer {
	return &Enqueuer{repository: repository}
}

// Enqueue persists a new job. If input.IdempotencyKey is set and already
// exists for the queue, the existing job is returned instead of a duplicate.
func (e *Enqueuer) Enqueue(ctx context.Context, input port.EnqueueInput) (domain.Job, error) {
	if input.Queue == "" {
		return domain.Job{}, fmt.Errorf("enqueuing job: queue is required")
	}
	if len(input.Payload) == 0 {
		return domain.Job{}, fmt.Errorf("enqueuing job: payload is required")
	}

	job, err := e.repository.Enqueue(ctx, input)
	if err != nil {
		return domain.Job{}, fmt.Errorf("enqueuing job: %w", err)
	}
	return job, nil
}
