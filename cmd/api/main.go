package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	httpadapter "hopper/internal/adapter/handler/http"
	postgresrepo "hopper/internal/adapter/repository/postgres"
	"hopper/internal/infrastructure/config"
	"hopper/internal/infrastructure/database"
	"hopper/internal/infrastructure/logger"
	"hopper/internal/infrastructure/server"
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
		logr.Error("api exited with error", "error", err)
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
	enqueuer := job.NewEnqueuer(repository)
	query := job.NewQuery(repository)

	jobHandler := httpadapter.NewJobHandler(enqueuer, query, cfg.Retry.MaxAttempts)
	e := httpadapter.NewRouter(jobHandler, pool, log, httpadapter.RouterConfig{
		RequestTimeout: cfg.Server.WriteTimeout,
		BodyLimit:      cfg.Server.BodyLimit,
	})

	log.Info("api started", "app", cfg.App.Name, "env", cfg.App.Env, "addr", fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port))
	if err := server.Run(ctx, e, cfg.Server, cfg.App.ShutdownTimeout); err != nil {
		return fmt.Errorf("running http server: %w", err)
	}
	log.Info("api stopped")
	return nil
}
