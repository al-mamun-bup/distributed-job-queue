//go:build integration

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	postgresrepo "hopper/internal/adapter/repository/postgres"
	"hopper/internal/domain"
	"hopper/internal/infrastructure/database"
	"hopper/internal/usecase/job"
	"hopper/internal/usecase/port"
)

func setupAPI(t *testing.T) (*echo.Echo, *postgresrepo.JobRepository) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(
		ctx,
		"postgres:16-alpine",
		tcpostgres.BasicWaitStrategies(),
		tcpostgres.WithDatabase("hopper"),
		tcpostgres.WithUsername("hopper"),
		tcpostgres.WithPassword("hopper"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, database.MigrateUp(ctx, pool))

	repository := postgresrepo.NewJobRepository(pool)
	enqueuer := job.NewEnqueuer(repository)
	query := job.NewQuery(repository)

	jobHandler := NewJobHandler(enqueuer, query, 5)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	e := NewRouter(jobHandler, pool, log, RouterConfig{RequestTimeout: 5 * time.Second, BodyLimit: "1M"})

	return e, repository
}

func doRequest(e *echo.Echo, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

// makeDeadJob enqueues a job and drives it to state=dead via claim+fail, the
// same path a real worker would take after exhausting attempts.
func makeDeadJob(t *testing.T, ctx context.Context, repository *postgresrepo.JobRepository, queue domain.Queue) domain.Job {
	t.Helper()

	created, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       queue,
		Payload:     []byte(`{}`),
		MaxAttempts: 1,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	claimed, err := repository.Claim(ctx, queue, 1, 5*time.Second, "test-worker")
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	require.NoError(t, repository.Fail(ctx, port.FailInput{
		ID:        created.ID,
		WorkerID:  "test-worker",
		RunAt:     time.Now().UTC(),
		LastError: "boom",
		MarkDead:  true,
	}))

	dead, err := repository.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, domain.JobStateDead, dead.State)
	return dead
}

func TestCreateAndGetJob(t *testing.T) {
	e, _ := setupAPI(t)

	body, err := json.Marshal(map[string]any{
		"queue":   "default",
		"payload": map[string]any{"n": 1},
	})
	require.NoError(t, err)

	rec := doRequest(e, "POST", "/v1/jobs", body, nil)
	require.Equal(t, 201, rec.Code)

	var created JobResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.NotEmpty(t, created.ID)
	require.Equal(t, "default", created.Queue)
	require.Equal(t, "pending", created.State)

	rec = doRequest(e, "GET", "/v1/jobs/"+created.ID, nil, nil)
	require.Equal(t, 200, rec.Code)

	var fetched JobResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fetched))
	require.Equal(t, created.ID, fetched.ID)
}

func TestCreateJobIdempotencyKeyReturnsSameJob(t *testing.T) {
	e, _ := setupAPI(t)

	body, err := json.Marshal(map[string]any{
		"queue":   "default",
		"payload": map[string]any{"n": 1},
	})
	require.NoError(t, err)

	headers := map[string]string{"Idempotency-Key": "same-key"}
	first := doRequest(e, "POST", "/v1/jobs", body, headers)
	require.Equal(t, 201, first.Code)

	second := doRequest(e, "POST", "/v1/jobs", body, headers)
	require.Equal(t, 201, second.Code)

	var firstJob, secondJob JobResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &firstJob))
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &secondJob))
	require.Equal(t, firstJob.ID, secondJob.ID)
}

func TestCreateJobValidationError(t *testing.T) {
	e, _ := setupAPI(t)

	body, err := json.Marshal(map[string]any{"payload": map[string]any{"n": 1}})
	require.NoError(t, err)

	rec := doRequest(e, "POST", "/v1/jobs", body, nil)
	require.Equal(t, 400, rec.Code)
}

func TestGetJobNotFound(t *testing.T) {
	e, _ := setupAPI(t)

	rec := doRequest(e, "GET", "/v1/jobs/"+uuid.NewString(), nil, nil)
	require.Equal(t, 404, rec.Code)
}

func TestRetryDeadJobResurrectsIt(t *testing.T) {
	e, repository := setupAPI(t)
	ctx := context.Background()

	dead := makeDeadJob(t, ctx, repository, "retry-queue")

	rec := doRequest(e, "POST", fmt.Sprintf("/v1/jobs/%s/retry", dead.ID), nil, nil)
	require.Equal(t, 200, rec.Code)

	var retried JobResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &retried))
	require.Equal(t, "pending", retried.State)
	require.Equal(t, 0, retried.Attempts)
}

func TestRetryNonDeadJobConflicts(t *testing.T) {
	e, _ := setupAPI(t)

	body, err := json.Marshal(map[string]any{
		"queue":   "default",
		"payload": map[string]any{"n": 1},
	})
	require.NoError(t, err)

	created := doRequest(e, "POST", "/v1/jobs", body, nil)
	require.Equal(t, 201, created.Code)

	var job JobResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &job))

	rec := doRequest(e, "POST", fmt.Sprintf("/v1/jobs/%s/retry", job.ID), nil, nil)
	require.Equal(t, 409, rec.Code)
}

func TestListJobsFiltersByState(t *testing.T) {
	e, repository := setupAPI(t)
	ctx := context.Background()

	dead := makeDeadJob(t, ctx, repository, "dead-letter-queue")

	rec := doRequest(e, "GET", "/v1/jobs?state=dead&queue=dead-letter-queue", nil, nil)
	require.Equal(t, 200, rec.Code)

	var page ListJobsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.GreaterOrEqual(t, page.Total, int64(1))

	found := false
	for _, j := range page.Jobs {
		require.Equal(t, "dead", j.State)
		if j.ID == dead.ID {
			found = true
		}
	}
	require.True(t, found)
}

func TestListJobsRejectsInvalidState(t *testing.T) {
	e, _ := setupAPI(t)

	rec := doRequest(e, "GET", "/v1/jobs?state=bogus", nil, nil)
	require.Equal(t, 400, rec.Code)
}

func TestQueueStats(t *testing.T) {
	e, repository := setupAPI(t)
	ctx := context.Background()

	_, err := repository.Enqueue(ctx, port.EnqueueInput{
		Queue:       "stats-queue",
		Payload:     []byte(`{}`),
		MaxAttempts: 5,
		RunAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	rec := doRequest(e, "GET", "/v1/queues/stats-queue/stats", nil, nil)
	require.Equal(t, 200, rec.Code)

	var stats QueueStatsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stats))
	require.Equal(t, "stats-queue", stats.Queue)
	require.Equal(t, int64(1), stats.Pending)
	require.Equal(t, int64(1), stats.Total)
}

func TestHealthzAndReadyz(t *testing.T) {
	e, _ := setupAPI(t)

	rec := doRequest(e, "GET", "/healthz", nil, nil)
	require.Equal(t, 200, rec.Code)

	rec = doRequest(e, "GET", "/readyz", nil, nil)
	require.Equal(t, 200, rec.Code)
}
