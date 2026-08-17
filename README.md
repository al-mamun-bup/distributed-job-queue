# hopper

A distributed job queue built in Go on top of Postgres. No Redis, no broker —
Postgres `SELECT ... FOR UPDATE SKIP LOCKED` does the job claiming, and
`LISTEN`/`NOTIFY` wakes workers up instead of them polling in a tight loop.

Built with Clean Architecture: domain logic (`internal/domain`) has zero
framework dependencies, use cases (`internal/usecase`) orchestrate through
small ports/interfaces, and adapters (`internal/adapter`) plug in Postgres
and Echo at the edges.

## How it works

- **Enqueue** — `POST /v1/jobs` inserts a row in state `pending`, optionally
  with an `Idempotency-Key` header so retried requests don't double-enqueue.
- **Claim** — each worker atomically claims a batch of due, pending jobs from
  its queues via `SELECT ... FOR UPDATE SKIP LOCKED`, so concurrent workers
  never grab the same job. A claimed job gets a lease (`lease_expires_at`)
  and moves to `running`.
- **Wake-up** — workers `LISTEN` on a Postgres channel per queue; enqueuing a
  job sends `NOTIFY`, so idle workers pick up new work immediately instead of
  waiting out their poll interval.
- **Execute** — the job handler runs with a per-job timeout. Success marks
  the job `succeeded`; failure computes the next retry time with exponential
  backoff + jitter (`internal/domain/backoff.go`) and requeues it, up to
  `max_attempts`, after which it's marked `dead`.
- **Reap** — a background reaper periodically returns jobs whose lease
  expired without a heartbeat (worker crashed or got stuck) back to
  `pending`, so no job is silently lost.
- **Observe** — both processes expose Prometheus metrics: queue depth by
  state, enqueue/completion counters, job duration histograms, claim batch
  sizes, and reclaimed-lease counts.

## Architecture

```
cmd/
  api/       HTTP entrypoint  — enqueue, query, retry jobs
  worker/    Worker entrypoint — claim loop, reaper, metrics server
  migrate/   Runs embedded SQL migrations (up/down)
internal/
  domain/          Job entity, state transitions, backoff math — no deps
  usecase/job/      Enqueuer, Query, Worker (claim loop), Reaper, Processor
  usecase/port/     Interfaces the use cases depend on (repository, metrics)
  adapter/handler/http/   Echo routes, DTOs, error mapping
  adapter/repository/postgres/  pgx-backed repository, LISTEN/NOTIFY listener
  infrastructure/  config, logger, metrics registry, DB pool, HTTP server
migrations/    Embedded SQL migrations
```

## Quickstart (Docker)

Requires Docker and Docker Compose.

```bash
make serve
# or: docker compose up --build -d
```

This starts Postgres, runs migrations once, then starts the API
(`:8080`) and worker (metrics on `:9090`).

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","payload":{"hello":"world"}}'
curl -s localhost:8080/v1/queues/default/stats
curl -s localhost:9090/metrics | grep hopper_
```

Tear down:

```bash
make down
# or: docker compose down -v
```

## Local development (without Docker)

Requires Go 1.25+ and a running Postgres.

```bash
make dev            # starts just Postgres in Docker
make migrate-up      # applies migrations
go run ./cmd/api      # in one terminal
go run ./cmd/worker   # in another
```

Config is read from `config/config.yaml` (copy from
`config/config.example.yaml`) and can be overridden per-field with
`HOPPER_`-prefixed env vars, e.g. `HOPPER_DATABASE_HOST=localhost`,
`HOPPER_DATABASE_PASSWORD=secret` (nested keys join with `_`).

## API

| Method | Path                      | Description                          |
|--------|---------------------------|---------------------------------------|
| GET    | `/healthz`                | Liveness                              |
| GET    | `/readyz`                 | Readiness (pings Postgres)            |
| GET    | `/metrics`                | Prometheus metrics                    |
| POST   | `/v1/jobs`                | Enqueue a job                         |
| GET    | `/v1/jobs`                | List jobs (`queue`, `state`, `limit`, `offset`) |
| GET    | `/v1/jobs/:id`             | Get a job                             |
| POST   | `/v1/jobs/:id/retry`       | Resurrect a `dead` job back to `pending` |
| GET    | `/v1/queues/:queue/stats` | Per-queue counts by state             |

`POST /v1/jobs` body:

```json
{
  "queue": "default",
  "payload": { "any": "json" },
  "max_attempts": 5,
  "run_at": "2026-01-01T00:00:00Z"
}
```

`max_attempts` and `run_at` are optional; `max_attempts` defaults to
`retry.max_attempts` in config, `run_at` defaults to now. Set an
`Idempotency-Key` header to make retried enqueue calls safe.

## Testing

```bash
make test       # unit tests
make test-int   # + integration tests (spins up Postgres via testcontainers, needs Docker)
make lint       # golangci-lint
```

CI runs all three plus a Docker image build on every push and PR — see
`.github/workflows/ci.yml`.

## Configuration reference

See `config/config.example.yaml` for the full set of tunables: server
timeouts, DB pool sizing, worker concurrency/batch size/lease TTL, reaper
interval, retry backoff (base delay, max delay, jitter), and log
level/format. Every key can be overridden with a `HOPPER_<SECTION>_<KEY>`
env var (see `docker-compose.yml` for an example).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
