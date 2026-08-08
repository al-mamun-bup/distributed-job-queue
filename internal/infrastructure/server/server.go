// Package server bootstraps HTTP serving.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"hopper/internal/infrastructure/config"
)

// Run serves e until ctx is done, then drains in-flight requests within
// shutdownTimeout via e.Shutdown before returning.
func Run(ctx context.Context, e *echo.Echo, cfg config.ServerConfig, shutdownTimeout time.Duration) error {
	e.Server.ReadTimeout = cfg.ReadTimeout
	e.Server.WriteTimeout = cfg.WriteTimeout

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	serveErrCh := make(chan error, 1)
	go func() {
		if err := e.Start(addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrCh <- fmt.Errorf("serving http: %w", err)
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := e.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutting down http server: %w", err)
	}
	return nil
}
