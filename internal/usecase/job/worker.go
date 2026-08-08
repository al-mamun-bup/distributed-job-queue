package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

type HandlerFunc func(ctx context.Context, job domain.Job) error

type WorkerConfig struct {
	WorkerID        string
	Queues          []domain.Queue
	Concurrency     int
	BatchSize       int
	PollInterval    time.Duration
	LeaseTTL        time.Duration
	JobTimeout      time.Duration
	ShutdownTimeout time.Duration
}

type Worker struct {
	repository port.JobRepository
	processor  *Processor
	handler    HandlerFunc
	notifier   port.Notifier
	cfg        WorkerConfig
}

// NewWorker wires a claim loop for cfg.Queues. notifier may be nil, in which
// case the worker falls back to pure polling at PollInterval.
func NewWorker(repository port.JobRepository, processor *Processor, handler HandlerFunc, notifier port.Notifier, cfg WorkerConfig) (*Worker, error) {
	if handler == nil {
		return nil, fmt.Errorf("creating worker: handler is required")
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
		if _, err := w.repository.ReleaseLeases(releaseCtx, w.cfg.WorkerID, jobIDs); err != nil {
			return fmt.Errorf("releasing still-held leases: %w", err)
		}
		return nil
	}
}

func (w *Worker) processClaimedJob(parentCtx context.Context, job domain.Job) {
	jobCtx, cancel := context.WithTimeout(parentCtx, w.cfg.JobTimeout)
	defer cancel()

	heartbeatStop := make(chan struct{})
	go func() {
		defer close(heartbeatStop)
		ticker := time.NewTicker(w.cfg.LeaseTTL / 3)
		defer ticker.Stop()

		for {
			select {
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				if err := w.repository.ExtendLease(jobCtx, job.ID, w.cfg.WorkerID, w.cfg.LeaseTTL); err != nil {
					return
				}
			}
		}
	}()

	var handlerErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				handlerErr = fmt.Errorf("handler panic: %v", recovered)
			}
		}()
		handlerErr = w.handler(jobCtx, job)
	}()

	cancel()
	<-heartbeatStop

	if handlerErr != nil {
		if err := w.processor.HandleFailure(context.Background(), job, w.cfg.WorkerID, handlerErr); err != nil {
			return
		}
		return
	}

	_ = w.processor.HandleSuccess(context.Background(), job.ID, w.cfg.WorkerID)
}
