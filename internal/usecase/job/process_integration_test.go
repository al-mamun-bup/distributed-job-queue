//go:build integration

package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// testLogger discards output; these tests assert on state, not log lines.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestReaperRedeliversExpiredLease(t *testing.T) {
	ctx := context.Background()
	repository, pool := setupUsecaseRepository(t, ctx)

	created, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "default",
		Payload:     []byte(`{"kind":"expired-lease"}`),
		MaxAttempts: 5,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	claimed, err := repository.Claim(ctx, "default", 1, 30*time.Second, "worker-a")
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, created.ID, claimed[0].ID)
	require.Equal(t, domain.JobStateRunning, claimed[0].State)

	_, err = pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, created.ID)
	require.NoError(t, err)

	reaper, err := NewReaper(repository, 100, nil, testLogger())
	require.NoError(t, err)
	reaped, err := reaper.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, reaped, 1)
	require.Equal(t, created.ID, reaped[0].ID)
	require.Equal(t, domain.JobStatePending, reaped[0].State)

	reclaimed, err := repository.Claim(ctx, "default", 1, 30*time.Second, "worker-b")
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, created.ID, reclaimed[0].ID)
}

func TestFailurePathDeadAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	repository, pool := setupUsecaseRepository(t, ctx)

	created, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "default",
		Payload:     []byte(`{"kind":"retry-me"}`),
		MaxAttempts: 5,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	retryPolicy := domain.RetryPolicy{
		BaseDelay: 10 * time.Second,
		MaxDelay:  time.Hour,
		Jitter:    0.2,
	}
	processor := NewProcessor(repository, retryPolicy)
	processor.SetJitterSource(func() float64 { return 0.5 })

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	expectedDelays := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
	}

	for attempt := 1; attempt <= 5; attempt++ {
		claimed, err := repository.Claim(ctx, "default", 1, 30*time.Second, "worker-retry")
		require.NoError(t, err)
		require.Len(t, claimed, 1, "attempt %d should claim one job", attempt)

		job := claimed[0]
		now := base.Add(time.Duration(attempt) * time.Minute)
		processor.SetClock(func() time.Time { return now })

		failureErr := errors.New(fmt.Sprintf("attempt %d failed", attempt))
		resultState, err := processor.HandleFailure(ctx, job, "worker-retry", failureErr)
		require.NoError(t, err)
		if attempt < 5 {
			require.Equal(t, domain.JobStatePending, resultState)
		} else {
			require.Equal(t, domain.JobStateDead, resultState)
		}

		updated, err := repository.Get(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, failureErr.Error(), updated.LastError)

		if attempt < 5 {
			require.Equal(t, domain.JobStatePending, updated.State)
			require.WithinDuration(t, now.Add(expectedDelays[attempt-1]), updated.RunAt, 2*time.Second)

			_, err = pool.Exec(ctx, `UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = $1`, created.ID)
			require.NoError(t, err)
			continue
		}

		require.Equal(t, domain.JobStateDead, updated.State)
		require.NotNil(t, updated.CompletedAt)
	}
}

func setupUsecaseRepository(t *testing.T, ctx context.Context) (*postgresrepo.JobRepository, *pgxpool.Pool) {
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
