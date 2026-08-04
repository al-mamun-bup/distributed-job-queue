package job

import (
	"context"
	"fmt"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

// Reaper requeues jobs whose running lease has expired.
type Reaper struct {
	repository port.JobRepository
	batchSize  int
}

func NewReaper(repository port.JobRepository, batchSize int) *Reaper {
	return &Reaper{
		repository: repository,
		batchSize:  batchSize,
	}
}

func (r *Reaper) RunOnce(ctx context.Context) ([]domain.Job, error) {
	jobs, err := r.repository.ReapExpired(ctx, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("reaping expired jobs: %w", err)
	}
	return jobs, nil
}
