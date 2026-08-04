package main

import (
	"context"
	"log/slog"
	"os"

	"hopper/internal/infrastructure/config"
	"hopper/internal/infrastructure/logger"
)

func main() {
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

	logr.InfoContext(context.Background(), "api skeleton initialized", "app", cfg.App.Name, "env", cfg.App.Env)
}
