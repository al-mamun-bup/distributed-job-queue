package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	postgresrepo "hopper/internal/adapter/repository/postgres"
	"hopper/internal/domain"
	"hopper/internal/infrastructure/config"
	"hopper/internal/infrastructure/database"
	"hopper/internal/infrastructure/logger"
	"hopper/internal/infrastructure/metrics"
	"hopper/internal/usecase/job"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		slog.Error("loading config", "error", err)
		os.Exit(1)
	}

	logr, err := logger.New(cfg.Log)
	if err != nil {
		slog.Error("creating logger", "error", err)
		os.Exit(1)
	}

	if err := run(rootCtx, cfg, logr); err != nil && !errors.Is(err, context.Canceled) {
		logr.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	pool, err := database.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("opening postgres pool: %w", err)
	}
	defer pool.Close()

	repository := postgresrepo.NewJobRepository(pool)
	processor := job.NewProcessor(repository, domain.RetryPolicy{
		BaseDelay: cfg.Retry.BaseDelay,
		MaxDelay:  cfg.Retry.MaxDelay,
		Jitter:    cfg.Retry.Jitter,
	})

	queues := make([]domain.Queue, 0, len(cfg.Worker.Queues))
	for _, queue := range cfg.Worker.Queues {
		queues = append(queues, domain.Queue(queue))
	}

	notifier, err := postgresrepo.NewQueueListener(pool, postgresrepo.NewJobChannel)
	if err != nil {
		return fmt.Errorf("creating queue listener: %w", err)
	}

	m := metrics.New()
	m.RegisterQueueDepth(repository)

	worker, err := job.NewWorker(repository, processor, defaultHandler(log), notifier, m, log, job.WorkerConfig{
		WorkerID:          cfg.Worker.ID,
		Queues:            queues,
		Concurrency:       cfg.Worker.Concurrency,
		BatchSize:         cfg.Worker.BatchSize,
		PollInterval:      cfg.Worker.PollInterval,
		LeaseTTL:          cfg.Worker.LeaseTTL,
		HeartbeatInterval: cfg.Worker.HeartbeatInterval,
		JobTimeout:        cfg.Worker.JobTimeout,
		ShutdownTimeout:   cfg.App.ShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("creating worker: %w", err)
	}

	reaper, err := job.NewReaper(repository, cfg.Reaper.BatchSize, m, log)
	if err != nil {
		return fmt.Errorf("creating reaper: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runReaperLoop(ctx, reaper, cfg.Reaper.Interval, log)
	}()
	go func() {
		defer wg.Done()
		if err := metrics.Serve(ctx, m.Registry, cfg.Metrics.Port); err != nil {
			log.Error("metrics server exited with error", "error", err)
		}
	}()

	log.Info("worker started",
		"worker_id", cfg.Worker.ID, "queues", cfg.Worker.Queues, "concurrency", cfg.Worker.Concurrency, "metrics_port", cfg.Metrics.Port)
	startedAt := time.Now()
	workerErr := worker.Run(ctx)
	wg.Wait()

	if workerErr != nil {
		return fmt.Errorf("running worker: %w", workerErr)
	}
	log.Info("worker stopped", "uptime", time.Since(startedAt).String())
	return nil
}

func runReaperLoop(ctx context.Context, reaper *job.Reaper, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := reaper.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("reaping expired leases", "error", err)
			}
		}
	}
}

func defaultHandler(log *slog.Logger) job.HandlerFunc {
	return func(ctx context.Context, j domain.Job) error {
		log.InfoContext(ctx, "processing job", "job_id", j.ID, "queue", j.Queue, "attempt", j.Attempts)
		return nil
	}
}
