package http

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Pinger checks connectivity to the datastore backing readiness. Defined
// here, the consumer, rather than alongside the pgx pool that implements it.
type Pinger interface {
	Ping(ctx context.Context) error
}

// RouterConfig controls middleware behaviour that varies by environment.
type RouterConfig struct {
	RequestTimeout time.Duration
	BodyLimit      string
}

type requestValidator struct {
	validate *validator.Validate
}

func (v *requestValidator) Validate(i any) error {
	if err := v.validate.Struct(i); err != nil {
		return err
	}
	return nil
}

// NewRouter wires middleware and routes onto a fresh Echo instance. DTOs and
// domain-error-to-status mapping live in this package; nothing upstream of
// the handler ever sees an HTTP status code.
func NewRouter(jobHandler *JobHandler, pinger Pinger, log *slog.Logger, cfg RouterConfig) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Validator = &requestValidator{validate: validator.New()}
	e.HTTPErrorHandler = NewHTTPErrorHandler(log)

	e.Use(middleware.RequestID())
	e.Use(middleware.Recover())
	e.Use(requestLogger(log))
	e.Use(middleware.ContextTimeoutWithConfig(middleware.ContextTimeoutConfig{
		Timeout: cfg.RequestTimeout,
	}))
	e.Use(middleware.BodyLimit(cfg.BodyLimit))
	e.Use(middleware.CORS())

	e.GET("/healthz", healthzHandler)
	e.GET("/readyz", readyzHandler(pinger))
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	v1 := e.Group("/v1")
	v1.POST("/jobs", jobHandler.Create)
	v1.GET("/jobs", jobHandler.List)
	v1.GET("/jobs/:id", jobHandler.Get)
	v1.POST("/jobs/:id/retry", jobHandler.Retry)
	v1.GET("/queues/:queue/stats", jobHandler.QueueStats)

	return e
}

func healthzHandler(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func readyzHandler(pinger Pinger) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
		defer cancel()

		if err := pinger.Ping(ctx); err != nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	}
}

func requestLogger(log *slog.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)

			req := c.Request()
			res := c.Response()
			status := res.Status
			if err != nil {
				var httpErr *echo.HTTPError
				if errors.As(err, &httpErr) {
					status = httpErr.Code
				} else if status == 0 {
					status = http.StatusInternalServerError
				}
			}

			log.InfoContext(req.Context(), "http request",
				"method", req.Method,
				"path", c.Path(),
				"status", status,
				"duration", time.Since(start).String(),
				"request_id", res.Header().Get(echo.HeaderXRequestID),
			)
			return err
		}
	}
}
