//go:build integration

package job

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	postgresrepo "hopper/internal/adapter/repository/postgres"
	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

// TestWorkerNotifyVsPollLatency records enqueue-to-handler-start latency with
// and without a Notifier wired in, to demonstrate NOTIFY is a latency win on
// top of polling (not a replacement for it).
func TestWorkerNotifyVsPollLatency(t *testing.T) {
	ctx := context.Background()
	repository, pool := setupWorkerRepository(t, ctx)

	retryPolicy := domain.RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Jitter: 0.2}

	measure := func(t *testing.T, queue domain.Queue, notifier port.Notifier) time.Duration {
		t.Helper()

		var (
			mu      sync.Mutex
			started time.Time
		)
		startedCh := make(chan struct{})
		handler := func(_ context.Context, _ domain.Job) error {
			mu.Lock()
			if started.IsZero() {
				started = time.Now()
				close(startedCh)
			}
			mu.Unlock()
			return nil
		}

		processor := NewProcessor(repository, retryPolicy)
		worker, err := NewWorker(repository, processor, handler, notifier, nil, testLogger(), WorkerConfig{
			WorkerID:        fmt.Sprintf("latency-%s", queue),
			Queues:          []domain.Queue{queue},
			Concurrency:     4,
			BatchSize:       4,
			PollInterval:    2 * time.Second,
			LeaseTTL:        5 * time.Second,
			JobTimeout:      10 * time.Second,
			ShutdownTimeout: 2 * time.Second,
		})
		require.NoError(t, err)

		runCtx, cancel := context.WithCancel(ctx)
		runDone := make(chan error, 1)
		go func() { runDone <- worker.Run(runCtx) }()

		// Let the empty-poll backoff climb to its ceiling so the worker is
		// parked in a long wait when we enqueue - otherwise both cases would
		// look artificially close together.
		time.Sleep(3 * time.Second)

		enqueuedAt := time.Now()
		_, err = repository.Enqueue(ctx, port.EnqueueInput{
			Queue:       queue,
			Payload:     []byte(`{}`),
			MaxAttempts: 1,
			RunAt:       time.Now().UTC(),
		})
		require.NoError(t, err)

		select {
		case <-startedCh:
		case <-time.After(6 * time.Second):
			t.Fatal("timed out waiting for handler to start")
		}

		cancel()
		require.NoError(t, <-runDone)

		mu.Lock()
		defer mu.Unlock()
		return started.Sub(enqueuedAt)
	}

	listener, err := postgresrepo.NewQueueListener(pool, postgresrepo.NewJobChannel)
	require.NoError(t, err)

	withNotify := measure(t, "latency-notify", listener)
	withoutNotify := measure(t, "latency-poll-only", nil)

	t.Logf("enqueue-to-start latency: with NOTIFY=%s, poll-only=%s", withNotify, withoutNotify)
	require.Less(t, withNotify, withoutNotify, "NOTIFY should wake the worker well before the poll timer would")
}

// TestWorkerListenerDropDoesNotLoseJobs kills the worker's LISTEN connection
// mid-run and asserts every job enqueued afterward is still delivered via the
// poll fallback - NOTIFY is an optimisation, polling is the correctness floor.
func TestWorkerListenerDropDoesNotLoseJobs(t *testing.T) {
	ctx := context.Background()
	repository, pool := setupWorkerRepository(t, ctx)

	listener, err := postgresrepo.NewQueueListener(pool, postgresrepo.NewJobChannel)
	require.NoError(t, err)

	retryPolicy := domain.RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Jitter: 0.2}
	processor := NewProcessor(repository, retryPolicy)

	var (
		mu    sync.Mutex
		count int
	)
	handler := func(_ context.Context, _ domain.Job) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}

	worker, err := NewWorker(repository, processor, handler, listener, nil, testLogger(), WorkerConfig{
		WorkerID:        "listener-drop-test",
		Queues:          []domain.Queue{"listener-drop"},
		Concurrency:     4,
		BatchSize:       4,
		PollInterval:    500 * time.Millisecond,
		LeaseTTL:        5 * time.Second,
		JobTimeout:      10 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()

	require.Eventually(t, func() bool {
		return listener.BackendPID() != 0
	}, 5*time.Second, 20*time.Millisecond, "listener never established its LISTEN connection")

	_, err = pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, listener.BackendPID())
	require.NoError(t, err)

	const totalJobs = 20
	for i := 0; i < totalJobs; i++ {
		_, err := repository.Enqueue(ctx, port.EnqueueInput{
			Queue:       "listener-drop",
			Payload:     []byte(fmt.Sprintf(`{"i":%d}`, i)),
			MaxAttempts: 1,
			RunAt:       time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == totalJobs
	}, 10*time.Second, 50*time.Millisecond, "polling fallback should still deliver every job after the listener connection is killed")

	cancel()
	require.NoError(t, <-done)
}
