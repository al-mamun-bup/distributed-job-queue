package job

import (
	"context"
	"fmt"
	"log/slog"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

// Reaper requeues jobs whose running lease has expired.
type Reaper struct {
	repository port.JobRepository
	batchSize  int
	metrics    port.MetricsRecorder
	log        *slog.Logger
}

// NewReaper wires a lease reaper. metrics may be nil to disable metrics.
func NewReaper(repository port.JobRepository, batchSize int, metrics port.MetricsRecorder, log *slog.Logger) (*Reaper, error) {
	if log == nil {
		return nil, fmt.Errorf("creating reaper: logger is required")
	}
	return &Reaper{
		repository: repository,
		batchSize:  batchSize,
		metrics:    metrics,
		log:        log,
	}, nil
}

func (r *Reaper) RunOnce(ctx context.Context) ([]domain.Job, error) {
	jobs, err := r.repository.ReapExpired(ctx, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("reaping expired jobs: %w", err)
	}

	if len(jobs) > 0 {
		jobIDs := make([]string, 0, len(jobs))
		for _, j := range jobs {
			jobIDs = append(jobIDs, j.ID)
		}
		// A reaped lease means a worker went silent (SIGKILL, crash, network
		// partition) without releasing it - Warn, not Info, since it's the
		// system compensating for a failure, not routine operation.
		r.log.WarnContext(ctx, "reclaimed expired leases", "count", len(jobs), "job_ids", jobIDs)
	}

	if r.metrics != nil {
		r.metrics.LeasesReclaimed(len(jobs))
	}

	return jobs, nil
}
