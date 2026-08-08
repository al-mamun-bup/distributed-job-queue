package job

import (
	"context"
	"fmt"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

// Query answers read-only questions about jobs for the API layer.
type Query struct {
	repository port.JobRepository
}

func NewQuery(repository port.JobRepository) *Query {
	return &Query{repository: repository}
}

func (q *Query) Get(ctx context.Context, id string) (domain.Job, error) {
	job, err := q.repository.Get(ctx, id)
	if err != nil {
		return domain.Job{}, fmt.Errorf("getting job %s: %w", id, err)
	}
	return job, nil
}

func (q *Query) List(ctx context.Context, input port.ListJobsInput) (port.ListJobsOutput, error) {
	out, err := q.repository.List(ctx, input)
	if err != nil {
		return port.ListJobsOutput{}, fmt.Errorf("listing jobs: %w", err)
	}
	return out, nil
}

func (q *Query) Stats(ctx context.Context, queue domain.Queue) (port.QueueStats, error) {
	stats, err := q.repository.Stats(ctx, queue)
	if err != nil {
		return port.QueueStats{}, fmt.Errorf("getting stats for queue %s: %w", queue, err)
	}
	return stats, nil
}

// Retry resurrects a dead job back to pending for another attempt.
func (q *Query) Retry(ctx context.Context, id string) (domain.Job, error) {
	job, err := q.repository.Retry(ctx, id)
	if err != nil {
		return domain.Job{}, fmt.Errorf("retrying job %s: %w", id, err)
	}
	return job, nil
}
