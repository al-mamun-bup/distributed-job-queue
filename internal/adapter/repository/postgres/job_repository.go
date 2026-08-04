// Package postgres contains Postgres adapters for repository ports.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

type JobRepository struct {
	pool *pgxpool.Pool
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (r *JobRepository) Enqueue(ctx context.Context, input port.EnqueueInput) (domain.Job, error) {
	id := uuid.NewString()
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = 5
	}
	if input.RunAt.IsZero() {
		input.RunAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO jobs (id, queue, payload, state, attempts, max_attempts, run_at, idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3::jsonb, 'pending', 0, $4, $5, $6, now(), now())
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET idempotency_key = jobs.idempotency_key
		RETURNING id, queue, payload, state, attempts, max_attempts, run_at, lease_expires_at, worker_id, idempotency_key, last_error, created_at, updated_at, completed_at
	`

	row := r.pool.QueryRow(
		ctx,
		query,
		id,
		string(input.Queue),
		string(input.Payload),
		input.MaxAttempts,
		input.RunAt,
		input.IdempotencyKey,
	)

	job, err := scanJob(row)
	if err != nil {
		return domain.Job{}, fmt.Errorf("enqueueing job: %w", err)
	}

	return job, nil
}

func (r *JobRepository) Claim(
	ctx context.Context,
	queue domain.Queue,
	batchSize int,
	leaseTTL time.Duration,
	workerID string,
) ([]domain.Job, error) {
	interval := toPostgresInterval(leaseTTL)
	const query = `
		UPDATE jobs
		SET state = 'running',
		    lease_expires_at = now() + $3::interval,
		    worker_id = $4,
		    attempts = attempts + 1,
		    updated_at = now()
		WHERE id IN (
		  SELECT id FROM jobs
		  WHERE queue = $1 AND state = 'pending' AND run_at <= now()
		  ORDER BY run_at
		  LIMIT $2
		  -- SKIP LOCKED is required so concurrent workers can claim different rows
		  -- without serializing behind row locks and collapsing throughput.
		  FOR UPDATE SKIP LOCKED
		)
		RETURNING id, queue, payload, state, attempts, max_attempts, run_at, lease_expires_at, worker_id, idempotency_key, last_error, created_at, updated_at, completed_at
	`

	rows, err := r.pool.Query(ctx, query, string(queue), batchSize, interval, workerID)
	if err != nil {
		return nil, fmt.Errorf("claiming batch: %w", err)
	}
	defer rows.Close()

	jobs := make([]domain.Job, 0, batchSize)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning claimed job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating claimed jobs: %w", err)
	}

	return jobs, nil
}

func (r *JobRepository) Complete(ctx context.Context, id string, workerID string, completedAt time.Time) error {
	const query = `
		UPDATE jobs
		SET state = 'succeeded',
		    lease_expires_at = NULL,
		    worker_id = NULL,
		    last_error = NULL,
		    completed_at = $3,
		    updated_at = now()
		WHERE id = $1 AND state = 'running' AND worker_id = $2
	`
	cmdTag, err := r.pool.Exec(ctx, query, id, workerID, completedAt)
	if err != nil {
		return fmt.Errorf("completing job: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("completing job %s: %w", id, domain.ErrJobNotFound)
	}
	return nil
}

func (r *JobRepository) Fail(ctx context.Context, input port.FailInput) error {
	state := "pending"
	if input.MarkDead {
		state = "dead"
	}

	const query = `
		UPDATE jobs
		SET state = $3::job_state,
		    run_at = $4,
		    lease_expires_at = NULL,
		    worker_id = NULL,
		    last_error = $5,
		    completed_at = $6,
		    updated_at = now()
		WHERE id = $1 AND state = 'running' AND worker_id = $2
	`
	cmdTag, err := r.pool.Exec(
		ctx,
		query,
		input.ID,
		input.WorkerID,
		state,
		input.RunAt,
		input.LastError,
		input.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("failing job: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("failing job %s: %w", input.ID, domain.ErrJobNotFound)
	}
	return nil
}

func (r *JobRepository) ExtendLease(ctx context.Context, id string, workerID string, leaseTTL time.Duration) error {
	const query = `
		UPDATE jobs
		SET lease_expires_at = now() + $3::interval,
		    updated_at = now()
		WHERE id = $1 AND state = 'running' AND worker_id = $2
	`
	cmdTag, err := r.pool.Exec(ctx, query, id, workerID, toPostgresInterval(leaseTTL))
	if err != nil {
		return fmt.Errorf("extending lease: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("extending lease for job %s: %w", id, domain.ErrJobNotFound)
	}
	return nil
}

func (r *JobRepository) ReapExpired(ctx context.Context, batchSize int) ([]domain.Job, error) {
	const query = `
		UPDATE jobs
		SET state = 'pending',
		    lease_expires_at = NULL,
		    worker_id = NULL,
		    updated_at = now()
		WHERE id IN (
			SELECT id
			FROM jobs
			WHERE state = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, queue, payload, state, attempts, max_attempts, run_at, lease_expires_at, worker_id, idempotency_key, last_error, created_at, updated_at, completed_at
	`

	rows, err := r.pool.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("reaping expired jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]domain.Job, 0, batchSize)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning reaped job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating reaped jobs: %w", err)
	}
	return jobs, nil
}

func (r *JobRepository) Get(ctx context.Context, id string) (domain.Job, error) {
	const query = `
		SELECT id, queue, payload, state, attempts, max_attempts, run_at, lease_expires_at, worker_id, idempotency_key, last_error, created_at, updated_at, completed_at
		FROM jobs
		WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, query, id)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, fmt.Errorf("getting job %s: %w", id, domain.ErrJobNotFound)
		}
		return domain.Job{}, fmt.Errorf("getting job %s: %w", id, err)
	}
	return job, nil
}

func (r *JobRepository) Stats(ctx context.Context, queue domain.Queue) (port.QueueStats, error) {
	const query = `
		SELECT
			queue,
			COUNT(*) FILTER (WHERE state = 'pending')::bigint AS pending,
			COUNT(*) FILTER (WHERE state = 'running')::bigint AS running,
			COUNT(*) FILTER (WHERE state = 'succeeded')::bigint AS succeeded,
			COUNT(*) FILTER (WHERE state = 'dead')::bigint AS dead,
			COUNT(*)::bigint AS total,
			now() AS updated_at
		FROM jobs
		WHERE queue = $1
		GROUP BY queue
	`

	var out port.QueueStats
	err := r.pool.QueryRow(ctx, query, string(queue)).Scan(
		&out.Queue,
		&out.Pending,
		&out.Running,
		&out.Succeeded,
		&out.Dead,
		&out.Total,
		&out.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return port.QueueStats{Queue: string(queue), UpdatedAt: time.Now().UTC()}, nil
		}
		return port.QueueStats{}, fmt.Errorf("querying queue stats: %w", err)
	}

	return out, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (domain.Job, error) {
	var (
		job            domain.Job
		queue          string
		state          string
		payload        []byte
		leaseExpiresAt *time.Time
		workerID       *string
		idempotencyKey *string
		lastError      *string
		completedAt    *time.Time
	)

	if err := row.Scan(
		&job.ID,
		&queue,
		&payload,
		&state,
		&job.Attempts,
		&job.MaxAttempts,
		&job.RunAt,
		&leaseExpiresAt,
		&workerID,
		&idempotencyKey,
		&lastError,
		&job.CreatedAt,
		&job.UpdatedAt,
		&completedAt,
	); err != nil {
		return domain.Job{}, fmt.Errorf("scanning job row: %w", err)
	}

	compactPayload := json.RawMessage(payload)
	job.Payload = append([]byte(nil), compactPayload...)
	job.Queue = domain.Queue(queue)
	job.State = domain.JobState(state)
	job.LeaseExpiresAt = leaseExpiresAt
	job.WorkerID = workerID
	job.IdempotencyKey = idempotencyKey
	job.CompletedAt = completedAt
	if lastError != nil {
		job.LastError = *lastError
	}

	return job, nil
}

func toPostgresInterval(value time.Duration) string {
	return fmt.Sprintf("%.0f seconds", value.Seconds())
}
