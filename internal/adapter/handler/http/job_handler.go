// Package http contains Echo HTTP handlers and DTO mapping.
package http

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"

	"hopper/internal/domain"
	"hopper/internal/usecase/job"
	"hopper/internal/usecase/port"
)

// JobHandler exposes job operations over HTTP.
type JobHandler struct {
	enqueuer           *job.Enqueuer
	query              *job.Query
	defaultMaxAttempts int
	log                *slog.Logger
}

func NewJobHandler(enqueuer *job.Enqueuer, query *job.Query, defaultMaxAttempts int, log *slog.Logger) *JobHandler {
	return &JobHandler{enqueuer: enqueuer, query: query, defaultMaxAttempts: defaultMaxAttempts, log: log}
}

func (h *JobHandler) requestID(c echo.Context) string {
	return c.Response().Header().Get(echo.HeaderXRequestID)
}

func (h *JobHandler) Create(c echo.Context) error {
	var req CreateJobRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := c.Validate(&req); err != nil {
		return err
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = h.defaultMaxAttempts
	}

	runAt := time.Now().UTC()
	if req.RunAt != nil {
		runAt = req.RunAt.UTC()
	}

	var idempotencyKey *string
	if key := c.Request().Header.Get("Idempotency-Key"); key != "" {
		idempotencyKey = &key
	}

	created, err := h.enqueuer.Enqueue(c.Request().Context(), port.EnqueueInput{
		Queue:          domain.Queue(req.Queue),
		Payload:        []byte(req.Payload),
		MaxAttempts:    maxAttempts,
		RunAt:          runAt,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}

	h.log.InfoContext(c.Request().Context(), "job enqueued",
		"job_id", created.ID, "queue", string(created.Queue), "attempt", created.Attempts, "request_id", h.requestID(c))

	return c.JSON(http.StatusCreated, toJobResponse(created))
}

func (h *JobHandler) Get(c echo.Context) error {
	j, err := h.query.Get(c.Request().Context(), c.Param("id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, toJobResponse(j))
}

// Retry resurrects a dead job back to pending.
func (h *JobHandler) Retry(c echo.Context) error {
	j, err := h.query.Retry(c.Request().Context(), c.Param("id"))
	if err != nil {
		return err
	}

	h.log.InfoContext(c.Request().Context(), "job retried",
		"job_id", j.ID, "queue", string(j.Queue), "attempt", j.Attempts, "request_id", h.requestID(c))

	return c.JSON(http.StatusOK, toJobResponse(j))
}

func (h *JobHandler) QueueStats(c echo.Context) error {
	stats, err := h.query.Stats(c.Request().Context(), domain.Queue(c.Param("queue")))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, toQueueStatsResponse(stats))
}

var listableStates = map[domain.JobState]struct{}{
	domain.JobStatePending:   {},
	domain.JobStateRunning:   {},
	domain.JobStateSucceeded: {},
	domain.JobStateDead:      {},
}

func (h *JobHandler) List(c echo.Context) error {
	var state *domain.JobState
	if raw := c.QueryParam("state"); raw != "" {
		s := domain.JobState(raw)
		if _, ok := listableStates[s]; !ok {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid state filter")
		}
		state = &s
	}

	var queue *domain.Queue
	if raw := c.QueryParam("queue"); raw != "" {
		q := domain.Queue(raw)
		queue = &q
	}

	limit := 20
	if raw := c.QueryParam("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid limit")
		}
		limit = parsed
	}

	offset := 0
	if raw := c.QueryParam("offset"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid offset")
		}
		offset = parsed
	}

	out, err := h.query.List(c.Request().Context(), port.ListJobsInput{
		Queue:  queue,
		State:  state,
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return err
	}

	resp := ListJobsResponse{
		Jobs:   make([]JobResponse, 0, len(out.Jobs)),
		Total:  out.Total,
		Limit:  limit,
		Offset: offset,
	}
	for _, j := range out.Jobs {
		resp.Jobs = append(resp.Jobs, toJobResponse(j))
	}
	return c.JSON(http.StatusOK, resp)
}
