//go:build integration

package job

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

type completedKey struct {
	queue  string
	result string
}

type fakeMetrics struct {
	mu              sync.Mutex
	enqueued        map[string]int
	completed       map[completedKey]int
	claimBatches    []int
	leasesReclaimed int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		enqueued:  make(map[string]int),
		completed: make(map[completedKey]int),
	}
}

func (f *fakeMetrics) JobEnqueued(queue string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued[queue]++
}

func (f *fakeMetrics) JobCompleted(queue, result string, _ time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed[completedKey{queue, result}]++
}

func (f *fakeMetrics) ClaimBatch(_ string, size int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimBatches = append(f.claimBatches, size)
}

func (f *fakeMetrics) LeasesReclaimed(count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leasesReclaimed += count
}

func (f *fakeMetrics) completedCount(queue, result string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed[completedKey{queue, result}]
}

func (f *fakeMetrics) claimBatchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claimBatches)
}

var _ port.MetricsRecorder = (*fakeMetrics)(nil)

func TestWorkerRecordsMetricsForSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	repository, _ := setupWorkerRepository(t, ctx)

	okJob, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "metrics-worker",
		Payload:     []byte(`{"ok":true}`),
		MaxAttempts: 1,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	deadJob, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "metrics-worker",
		Payload:     []byte(`{"ok":false}`),
		MaxAttempts: 1,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	retryPolicy := domain.RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Jitter: 0.2}
	handler := func(_ context.Context, j domain.Job) error {
		if j.ID == deadJob.ID {
			return errors.New("boom")
		}
		return nil
	}

	fm := newFakeMetrics()
	processor := NewProcessor(repository, retryPolicy)
	worker, err := NewWorker(repository, processor, handler, nil, fm, testLogger(), WorkerConfig{
		WorkerID:        "metrics-worker-test",
		Queues:          []domain.Queue{"metrics-worker"},
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
	go func() { done <- worker.Run(runCtx) }()

	waitForState(t, repository, okJob.ID, domain.JobStateSucceeded, 5*time.Second)
	waitForState(t, repository, deadJob.ID, domain.JobStateDead, 5*time.Second)

	cancel()
	require.NoError(t, <-done)

	require.Equal(t, 1, fm.completedCount("metrics-worker", "succeeded"))
	require.Equal(t, 1, fm.completedCount("metrics-worker", "dead"))
	require.GreaterOrEqual(t, fm.claimBatchCount(), 1)
}

func TestReaperRecordsLeasesReclaimedMetric(t *testing.T) {
	ctx := context.Background()
	repository, pool := setupWorkerRepository(t, ctx)

	created, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "metrics-reaper",
		Payload:     []byte(`{}`),
		MaxAttempts: 5,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	claimed, err := repository.Claim(ctx, "metrics-reaper", 1, 30*time.Second, "worker-a")
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	_, err = pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, created.ID)
	require.NoError(t, err)

	fm := newFakeMetrics()
	reaper, err := NewReaper(repository, 100, fm, testLogger())
	require.NoError(t, err)

	_, err = reaper.RunOnce(ctx)
	require.NoError(t, err)

	fm.mu.Lock()
	defer fm.mu.Unlock()
	require.Equal(t, 1, fm.leasesReclaimed)
}
