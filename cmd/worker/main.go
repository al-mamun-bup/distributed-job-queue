package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	postgresrepo "hopper/internal/adapter/repository/postgres"
	"hopper/internal/domain"
	"hopper/internal/infrastructure/config"
	"hopper/internal/infrastructure/database"
	"hopper/internal/infrastructure/logger"
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

	worker, err := job.NewWorker(repository, processor, defaultHandler(log), notifier, job.WorkerConfig{
		WorkerID:        cfg.Worker.ID,
		Queues:          queues,
		Concurrency:     cfg.Worker.Concurrency,
		BatchSize:       cfg.Worker.BatchSize,
		PollInterval:    cfg.Worker.PollInterval,
		LeaseTTL:        cfg.Worker.LeaseTTL,
		JobTimeout:      cfg.Worker.JobTimeout,
		ShutdownTimeout: cfg.App.ShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("creating worker: %w", err)
	}

	log.Info("worker started", "worker_id", cfg.Worker.ID, "queues", cfg.Worker.Queues, "concurrency", cfg.Worker.Concurrency)
	startedAt := time.Now()
	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("running worker: %w", err)
	}
	log.Info("worker stopped", "uptime", time.Since(startedAt).String())
	return nil
}

func defaultHandler(log *slog.Logger) job.HandlerFunc {
	return func(ctx context.Context, j domain.Job) error {
		log.InfoContext(ctx, "processing job", "job_id", j.ID, "queue", j.Queue, "attempt", j.Attempts)
		return nil
	}
}
