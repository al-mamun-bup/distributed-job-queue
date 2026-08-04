// Package logger configures structured logging.
package logger

import (
	"fmt"
	"log/slog"
	"os"

	"hopper/internal/infrastructure/config"
)

// New returns a configured slog logger based on configured level and format.
func New(cfg config.LogConfig) (*slog.Logger, error) {
	level := new(slog.LevelVar)
	switch cfg.Level {
	case "debug":
		level.Set(slog.LevelDebug)
	case "info":
		level.Set(slog.LevelInfo)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		return nil, fmt.Errorf("unsupported log level: %s", cfg.Level)
	}

	opts := &slog.HandlerOptions{Level: level}
	switch cfg.Format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, opts)), nil
	default:
		return nil, fmt.Errorf("unsupported log format: %s", cfg.Format)
	}
}
