//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"hopper/internal/infrastructure/database"
	"hopper/internal/usecase/port"
)

func TestEnqueueHonorsIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	repo := setupRepository(t, ctx)
	idempotencyKey := "same-key"

	first, err := repo.Enqueue(ctx, port.EnqueueInput{
		Queue:          "default",
		Payload:        []byte(`{"n":1}`),
		MaxAttempts:    5,
		RunAt:          time.Now().UTC(),
		IdempotencyKey: &idempotencyKey,
	})
	require.NoError(t, err)

	second, err := repo.Enqueue(ctx, port.EnqueueInput{
		Queue:          "default",
		Payload:        []byte(`{"n":2}`),
		MaxAttempts:    5,
		RunAt:          time.Now().UTC(),
		IdempotencyKey: &idempotencyKey,
	})
	require.NoError(t, err)

	require.Equal(t, first.ID, second.ID)
	require.Equal(t, first.Payload, second.Payload)
}

func TestClaimConcurrentNoDuplicates(t *testing.T) {
	ctx := context.Background()
	repo := setupRepository(t, ctx)
	const (
		totalJobs = 1000
		workers   = 8
		batchSize = 10
		leaseTTL  = 30 * time.Second
	)

	for i := 0; i < totalJobs; i++ {
		_, err := repo.Enqueue(ctx, port.EnqueueInput{
			Queue:       "default",
			Payload:     []byte(fmt.Sprintf(`{"job":%d}`, i)),
			MaxAttempts: 5,
			RunAt:       time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	var (
		seen       sync.Map
		claimedCnt int64
	)
	errCh := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		go func(workerID string) {
			defer wg.Done()
			emptyPolls := 0
			for emptyPolls < 5 {
				jobs, err := repo.Claim(ctx, "default", batchSize, leaseTTL, workerID)
				if err != nil {
					errCh <- fmt.Errorf("worker %s claiming: %w", workerID, err)
					return
				}

				if len(jobs) == 0 {
					emptyPolls++
					time.Sleep(10 * time.Millisecond)
					continue
				}
				emptyPolls = 0

				for _, job := range jobs {
					_, loaded := seen.LoadOrStore(job.ID, workerID)
					if loaded {
						errCh <- fmt.Errorf("job claimed more than once: %s", job.ID)
						return
					}
					atomic.AddInt64(&claimedCnt, 1)
				}
			}
		}(workerID)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	require.Equal(t, int64(totalJobs), claimedCnt)

	seenCount := 0
	seen.Range(func(_, _ any) bool {
		seenCount++
		return true
	})
	require.Equal(t, totalJobs, seenCount)
}

func setupRepository(t *testing.T, ctx context.Context) *JobRepository {
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

	return NewJobRepository(pool)
}
