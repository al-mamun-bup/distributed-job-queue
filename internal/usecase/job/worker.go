package job

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

type HandlerFunc func(ctx context.Context, job domain.Job) error

type WorkerConfig struct {
	WorkerID          string
	Queues            []domain.Queue
	Concurrency       int
	BatchSize         int
	PollInterval      time.Duration
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	JobTimeout        time.Duration
	ShutdownTimeout   time.Duration
}

type Worker struct {
	repository port.JobRepository
	processor  *Processor
	handler    HandlerFunc
	notifier   port.Notifier
	metrics    port.MetricsRecorder
	log        *slog.Logger
	cfg        WorkerConfig
}

// NewWorker wires a claim loop for cfg.Queues. notifier and metrics may be
// nil: a nil notifier falls back to pure polling at PollInterval, a nil
// metrics disables metrics.
func NewWorker(repository port.JobRepository, processor *Processor, handler HandlerFunc, notifier port.Notifier, metrics port.MetricsRecorder, log *slog.Logger, cfg WorkerConfig) (*Worker, error) {
	if handler == nil {
		return nil, fmt.Errorf("creating worker: handler is required")
	}
	if log == nil {
		return nil, fmt.Errorf("creating worker: logger is required")
	}
	if len(cfg.Queues) == 0 {
		return nil, fmt.Errorf("creating worker: at least one queue is required")
	}
	if cfg.Concurrency <= 0 {
		return nil, fmt.Errorf("creating worker: concurrency must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return nil, fmt.Errorf("creating worker: batch size must be > 0")
	}
	if cfg.BatchSize > cfg.Concurrency {
		return nil, fmt.Errorf("creating worker: batch size must be <= concurrency")
	}
	if cfg.LeaseTTL <= 0 {
		return nil, fmt.Errorf("creating worker: lease ttl must be > 0")
	}
	if cfg.HeartbeatInterval < 0 {
		return nil, fmt.Errorf("creating worker: heartbeat interval must be >= 0")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = cfg.LeaseTTL / 3
	} else if cfg.HeartbeatInterval >= cfg.LeaseTTL {
		return nil, fmt.Errorf("creating worker: heartbeat interval must be < lease ttl")
	}
	if cfg.JobTimeout <= 0 {
		return nil, fmt.Errorf("creating worker: job timeout must be > 0")
	}
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("creating worker: poll interval must be > 0")
	}
	if cfg.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("creating worker: shutdown timeout must be > 0")
	}

	return &Worker{
		repository: repository,
		processor:  processor,
		handler:    handler,
		notifier:   notifier,
		metrics:    metrics,
		log:        log,
		cfg:        cfg,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	claimCtx, stopClaim := context.WithCancel(ctx)
	defer stopClaim()

	slots := make(chan struct{}, w.cfg.Concurrency)
	for i := 0; i < cap(slots); i++ {
		slots <- struct{}{}
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		inFlight = make(map[string]struct{}, w.cfg.Concurrency)
	)

	recordStart := func(jobID string) {
		mu.Lock()
		inFlight[jobID] = struct{}{}
		mu.Unlock()
	}
	recordDone := func(jobID string) {
		mu.Lock()
		delete(inFlight, jobID)
		mu.Unlock()
	}
	snapshotInFlight := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, 0, len(inFlight))
		for jobID := range inFlight {
			out = append(out, jobID)
		}
		return out
	}

	pollBackoff := 10 * time.Millisecond
	maxBackoff := w.cfg.PollInterval
	if maxBackoff < pollBackoff {
		maxBackoff = pollBackoff
	}

	// NOTIFY is a latency optimisation on top of polling, not a replacement
	// for it: pg_notify is fire-and-forget, so a signal sent while no
	// listener connection is up is gone forever, and delayed/backoff jobs
	// only ever become visible via run_at <= now(). The poll timer below
	// stays the correctness floor; notifications only let us skip waiting
	// out the rest of pollBackoff when one arrives.
	var notifications <-chan struct{}
	if w.notifier != nil {
		notifications = w.notifier.Notifications()
		go func() {
			_ = w.notifier.Run(claimCtx)
		}()
	}

	claimErrCh := make(chan error, 1)
	go func() {
		defer close(claimErrCh)
		queueIdx := 0
		for {
			select {
			case <-claimCtx.Done():
				return
			default:
			}

			queue := w.cfg.Queues[queueIdx%len(w.cfg.Queues)]
			queueIdx++

			available := len(slots)
			if available == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			batch := w.cfg.BatchSize
			if available < batch {
				batch = available
			}

			jobs, err := w.processor.Claim(claimCtx, queue, batch, w.cfg.LeaseTTL, w.cfg.WorkerID)
			if err != nil {
				select {
				case claimErrCh <- fmt.Errorf("claim loop: %w", err):
				default:
				}
				return
			}

			if len(jobs) == 0 {
				select {
				case <-claimCtx.Done():
					return
				case <-notifications:
					// Woken early: something was enqueued, go check right away.
					pollBackoff = 10 * time.Millisecond
				case <-time.After(pollBackoff):
					pollBackoff *= 2
					if pollBackoff > maxBackoff {
						pollBackoff = maxBackoff
					}
				}
				continue
			}
			pollBackoff = 10 * time.Millisecond

			if w.metrics != nil {
				w.metrics.ClaimBatch(string(queue), len(jobs))
			}

			for _, claimed := range jobs {
				<-slots
				recordStart(claimed.ID)

				wg.Add(1)
				go func(job domain.Job) {
					defer wg.Done()
					defer func() {
						recordDone(job.ID)
						slots <- struct{}{}
					}()
					// Detached from ctx/claimCtx on purpose: shutdown must let an
					// in-flight handler run to completion (or hit JobTimeout) rather
					// than cancel it the instant SIGTERM arrives. Draining within
					// ShutdownTimeout is enforced below via wg.Wait(), not via ctx.
					w.processClaimedJob(context.Background(), job)
				}(claimed)
			}
		}
	}()

	select {
	case err := <-claimErrCh:
		if err != nil {
			stopClaim()
			wg.Wait()
			return fmt.Errorf("worker run failed: %w", err)
		}
	case <-ctx.Done():
		stopClaim()
		// Wait for the claim loop goroutine to actually exit before
		// touching wg below. Without this, a batch it already claimed
		// could still be mid wg.Add() concurrently with wg.Wait(),
		// which is a data race on the WaitGroup's internal counter -
		// checking claimCtx.Done() only at the top of its loop isn't
		// enough on its own. claimErrCh is closed on return, so this
		// receive blocks exactly until that happens.
		<-claimErrCh
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-time.After(w.cfg.ShutdownTimeout):
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		jobIDs := snapshotInFlight()
		released, err := w.repository.ReleaseLeases(releaseCtx, w.cfg.WorkerID, jobIDs)
		if err != nil {
			return fmt.Errorf("releasing still-held leases: %w", err)
		}
		if released > 0 {
			w.log.WarnContext(releaseCtx, "shutdown timeout hit, released still-held leases",
				"worker_id", w.cfg.WorkerID, "count", released)
		}
		return nil
	}
}

func (w *Worker) processClaimedJob(parentCtx context.Context, job domain.Job) {
	jobCtx, cancel := context.WithTimeout(parentCtx, w.cfg.JobTimeout)
	defer cancel()

	jobAttrs := []any{"job_id", job.ID, "queue", string(job.Queue), "attempt", job.Attempts}

	heartbeatStop := make(chan struct{})
	go func() {
		defer close(heartbeatStop)
		ticker := time.NewTicker(w.cfg.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				if err := w.repository.ExtendLease(jobCtx, job.ID, w.cfg.WorkerID, w.cfg.LeaseTTL); err != nil {
					w.log.WarnContext(jobCtx, "extending lease failed", append(jobAttrs, "error", err)...)
					return
				}
			}
		}
	}()

	start := time.Now()
	var (
		handlerErr error
		panicked   bool
	)
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				panicked = true
				handlerErr = fmt.Errorf("handler panic: %v", recovered)
			}
		}()
		handlerErr = w.handler(jobCtx, job)
	}()
	duration := time.Since(start)

	cancel()
	<-heartbeatStop

	bgCtx := context.Background()

	if handlerErr != nil {
		if panicked {
			w.log.ErrorContext(bgCtx, "job handler panicked, recovered", append(jobAttrs, "error", handlerErr)...)
		}

		resultState, err := w.processor.HandleFailure(bgCtx, job, w.cfg.WorkerID, handlerErr)
		if err != nil {
			w.log.ErrorContext(bgCtx, "recording job failure failed", append(jobAttrs, "error", err)...)
			return
		}

		result := "retried"
		logLevel := slog.LevelWarn
		if resultState == domain.JobStateDead {
			result = "dead"
			logLevel = slog.LevelError
		}
		w.log.Log(bgCtx, logLevel, "job failed",
			append(jobAttrs, "result", result, "duration", duration.String(), "error", handlerErr)...)

		if w.metrics != nil {
			w.metrics.JobCompleted(string(job.Queue), result, duration)
		}
		return
	}

	if err := w.processor.HandleSuccess(bgCtx, job.ID, w.cfg.WorkerID); err != nil {
		w.log.ErrorContext(bgCtx, "recording job success failed", append(jobAttrs, "error", err)...)
		return
	}

	w.log.InfoContext(bgCtx, "job succeeded", append(jobAttrs, "duration", duration.String())...)
	if w.metrics != nil {
		w.metrics.JobCompleted(string(job.Queue), "succeeded", duration)
	}
}
