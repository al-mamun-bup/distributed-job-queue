package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

type jitterFn func() float64

// Processor handles state transitions after job execution attempts.
type Processor struct {
	repository port.JobRepository
	retry      domain.RetryPolicy
	now        func() time.Time
	jitter     jitterFn
}

func NewProcessor(repository port.JobRepository, retry domain.RetryPolicy) *Processor {
	return &Processor{
		repository: repository,
		retry:      retry,
		now:        time.Now,
		jitter: func() float64 {
			return 0.5
		},
	}
}

func (p *Processor) SetClock(nowFn func() time.Time) {
	p.now = nowFn
}

func (p *Processor) SetJitterSource(jitter jitterFn) {
	p.jitter = jitter
}

// HandleFailure records a handler error and returns the resulting state:
// JobStatePending if another attempt is scheduled, JobStateDead if attempts
// are exhausted.
func (p *Processor) HandleFailure(ctx context.Context, job domain.Job, workerID string, handlerErr error) (domain.JobState, error) {
	if handlerErr == nil {
		return "", fmt.Errorf("handling failure for job %s: handler error is required", job.ID)
	}
	if job.State != domain.JobStateRunning {
		return "", fmt.Errorf("handling failure for job %s from state %s: %w", job.ID, job.State, domain.ErrInvalidTransition)
	}

	now := p.now().UTC()
	failInput := port.FailInput{
		ID:        job.ID,
		WorkerID:  workerID,
		LastError: handlerErr.Error(),
	}

	if !job.CanRetry() {
		failInput.MarkDead = true
		failInput.RunAt = job.RunAt
		failInput.CompletedAt = &now
		if err := p.repository.Fail(ctx, failInput); err != nil {
			return "", fmt.Errorf("marking job %s dead: %w", job.ID, err)
		}
		return domain.JobStateDead, nil
	}

	// Jitter prevents failed jobs from retrying at the exact same instant.
	// Without it, an outage can trigger synchronized retries and a thundering herd.
	nextRunAt, err := domain.ComputeNextRunAt(now, job.Attempts, p.retry, p.jitter())
	if err != nil {
		return "", fmt.Errorf("computing next run_at for job %s: %w", job.ID, err)
	}
	failInput.RunAt = nextRunAt

	if err := p.repository.Fail(ctx, failInput); err != nil {
		return "", fmt.Errorf("marking job %s retryable failure: %w", job.ID, err)
	}
	return domain.JobStatePending, nil
}

func (p *Processor) HandleSuccess(ctx context.Context, jobID, workerID string) error {
	if err := p.repository.Complete(ctx, jobID, workerID, p.now().UTC()); err != nil {
		return fmt.Errorf("completing job %s: %w", jobID, err)
	}
	return nil
}

func (p *Processor) Claim(ctx context.Context, queue domain.Queue, batchSize int, leaseTTL time.Duration, workerID string) ([]domain.Job, error) {
	jobs, err := p.repository.Claim(ctx, queue, batchSize, leaseTTL, workerID)
	if err != nil {
		return nil, fmt.Errorf("claiming jobs from queue %s: %w", queue, err)
	}
	return jobs, nil
}

func (p *Processor) Get(ctx context.Context, jobID string) (domain.Job, error) {
	job, err := p.repository.Get(ctx, jobID)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			return domain.Job{}, err
		}
		return domain.Job{}, fmt.Errorf("getting job %s: %w", jobID, err)
	}
	return job, nil
}
