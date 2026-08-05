//go:build integration

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	postgresrepo "hopper/internal/adapter/repository/postgres"
	"hopper/internal/domain"
	"hopper/internal/infrastructure/database"
	"hopper/internal/usecase/port"
)

func TestWorkerGracefulShutdownMidBatchNoLossNoDuplicates(t *testing.T) {
	ctx := context.Background()
	repository, pool := setupWorkerRepository(t, ctx)

	const totalJobs = 120
	for i := 0; i < totalJobs; i++ {
		_, err := repository.Enqueue(ctx, port.EnqueueInput{
			Queue:       "default",
			Payload:     []byte(fmt.Sprintf(`{"job":%d}`, i)),
			MaxAttempts: 3,
			RunAt:       time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	retryPolicy := domain.RetryPolicy{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  time.Second,
		Jitter:    0.2,
	}
	executionCount := map[string]int{}
	var countMu sync.Mutex

	handler := func(_ context.Context, j domain.Job) error {
		countMu.Lock()
		executionCount[j.ID]++
		countMu.Unlock()
		time.Sleep(70 * time.Millisecond)
		return nil
	}

	processor1 := NewProcessor(repository, retryPolicy)
	worker1, err := NewWorker(repository, processor1, handler, nil, WorkerConfig{
		WorkerID:        "worker-a",
		Queues:          []domain.Queue{"default"},
		Concurrency:     10,
		BatchSize:       10,
		PollInterval:    100 * time.Millisecond,
		LeaseTTL:        5 * time.Second,
		JobTimeout:      30 * time.Second,
		ShutdownTimeout: 8 * time.Second,
	})
	require.NoError(t, err)

	worker1Ctx, cancelWorker1 := context.WithCancel(ctx)
	worker1Done := make(chan error, 1)
	go func() {
		worker1Done <- worker1.Run(worker1Ctx)
	}()

	time.Sleep(300 * time.Millisecond)
	cancelWorker1()
	require.NoError(t, <-worker1Done)

	processor2 := NewProcessor(repository, retryPolicy)
	worker2, err := NewWorker(repository, processor2, handler, nil, WorkerConfig{
		WorkerID:        "worker-b",
		Queues:          []domain.Queue{"default"},
		Concurrency:     10,
		BatchSize:       10,
		PollInterval:    100 * time.Millisecond,
		LeaseTTL:        5 * time.Second,
		JobTimeout:      30 * time.Second,
		ShutdownTimeout: 8 * time.Second,
	})
	require.NoError(t, err)

	worker2Ctx, cancelWorker2 := context.WithCancel(ctx)
	worker2Done := make(chan error, 1)
	go func() {
		worker2Done <- worker2.Run(worker2Ctx)
	}()

	waitForSucceededCount(t, pool, totalJobs, 10*time.Second)
	cancelWorker2()
	require.NoError(t, <-worker2Done)

	var (
		pending   int
		running   int
		succeeded int
		dead      int
	)
	err = pool.QueryRow(
		ctx,
		`SELECT
			COUNT(*) FILTER (WHERE state = 'pending')::int,
			COUNT(*) FILTER (WHERE state = 'running')::int,
			COUNT(*) FILTER (WHERE state = 'succeeded')::int,
			COUNT(*) FILTER (WHERE state = 'dead')::int
		 FROM jobs`,
	).Scan(&pending, &running, &succeeded, &dead)
	require.NoError(t, err)
	require.Equal(t, 0, pending)
	require.Equal(t, 0, running)
	require.Equal(t, totalJobs, succeeded)
	require.Equal(t, 0, dead)

	countMu.Lock()
	defer countMu.Unlock()
	require.Len(t, executionCount, totalJobs)
	for jobID, count := range executionCount {
		require.Equal(t, 1, count, "job %s executed %d times", jobID, count)
	}
}

func TestWorkerRecoversPanicAndKeepsRunning(t *testing.T) {
	ctx := context.Background()
	repository, _ := setupWorkerRepository(t, ctx)

	panicJob, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "default",
		Payload:     []byte(`{"panic":true}`),
		MaxAttempts: 1,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	okJob, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "default",
		Payload:     []byte(`{"panic":false}`),
		MaxAttempts: 3,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	retryPolicy := domain.RetryPolicy{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  time.Second,
		Jitter:    0.2,
	}
	handler := func(_ context.Context, j domain.Job) error {
		var payload struct {
			Panic bool `json:"panic"`
		}
		_ = json.Unmarshal(j.Payload, &payload)
		if payload.Panic {
			panic("poison pill")
		}
		return nil
	}

	processor := NewProcessor(repository, retryPolicy)
	worker, err := NewWorker(repository, processor, handler, nil, WorkerConfig{
		WorkerID:        "worker-panic-test",
		Queues:          []domain.Queue{"default"},
		Concurrency:     2,
		BatchSize:       2,
		PollInterval:    50 * time.Millisecond,
		LeaseTTL:        5 * time.Second,
		JobTimeout:      10 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx)
	}()

	waitForState(t, repository, panicJob.ID, domain.JobStateDead, 5*time.Second)
	waitForState(t, repository, okJob.ID, domain.JobStateSucceeded, 5*time.Second)

	cancel()
	require.NoError(t, <-done)
}

func setupWorkerRepository(t *testing.T, ctx context.Context) (*postgresrepo.JobRepository, *pgxpool.Pool) {
	t.Helper()

	container, err := tcpostgres.Run(
		ctx,
		"postgres:16-alpine",
		tcpostgres.BasicWaitStrategies(),
		tcpostgres.WithDatabase("hopper"),
		tcpostgres.WithUsername("hopper"),
		tcpostgres.WithPassword("hopper"),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, database.MigrateUp(ctx, pool))

	return postgresrepo.NewJobRepository(pool), pool
}

func waitForSucceededCount(t *testing.T, pool *pgxpool.Pool, expected int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var succeeded int
		err := pool.QueryRow(context.Background(), `SELECT COUNT(*)::int FROM jobs WHERE state = 'succeeded'`).Scan(&succeeded)
		require.NoError(t, err)
		if succeeded == expected {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for succeeded count %d", expected)
}

func waitForState(t *testing.T, repository *postgresrepo.JobRepository, jobID string, expected domain.JobState, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := repository.Get(context.Background(), jobID)
		require.NoError(t, err)
		if job.State == expected {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %s state %s", jobID, expected)
}
