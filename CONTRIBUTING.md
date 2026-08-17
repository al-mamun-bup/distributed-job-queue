# Contributing to hopper

## Setup

Requires Go 1.25+, Docker (for integration tests and `docker compose`).

```bash
git clone <repo-url>
cd distributed-job-queue
go mod download
cp config/config.example.yaml config/config.yaml
```

## Workflow

1. Fork/branch from `main`.
2. Make your change.
3. Run checks locally before opening a PR:

   ```bash
   make build
   make test        # unit tests
   make test-int    # integration tests, needs Docker
   make lint        # golangci-lint (see .golangci.yml)
   ```

4. Open a PR against `main`. CI (`.github/workflows/ci.yml`) runs build,
   unit tests, integration tests, lint, and a Docker image build — all must
   pass.

## Code style

- Clean Architecture boundaries are enforced by convention, not tooling —
  keep them:
  - `internal/domain` has no framework or infrastructure imports.
  - `internal/usecase` depends only on `internal/domain` and the interfaces
    in `internal/usecase/port`, never directly on Postgres or Echo types.
  - `internal/adapter/*` implements those ports and does all
    framework/driver-specific work (SQL, HTTP status codes, DTOs).
- No comments explaining *what* code does — name things clearly instead.
  Comment only non-obvious *why* (a workaround, an invariant, a subtle
  constraint).
- Don't add abstractions, config flags, or error handling for cases that
  can't happen — keep changes scoped to what's needed.
- Match existing patterns in the package you're touching before introducing
  a new one.

## Tests

- Unit tests live next to the code (`*_test.go`), no build tag.
- Integration tests use the `integration` build tag and testcontainers to
  spin up real Postgres — see `*_integration_test.go` files for examples.
  Run them with `make test-int`.
- Add tests for new domain logic and use-case behavior. Handler/repository
  changes should get integration test coverage.

## Commit messages

Follow the existing history: `type: short description` (e.g. `feat: ...`,
`fix: ...`, `refactor: ...`), imperative mood, why over what when it's not
obvious from the diff.

## Migrations

Add a new pair of files to `migrations/` (`NNNNNN_description.up.sql` /
`.down.sql`), numbered sequentially. They're embedded into the binary and
applied in order by `cmd/migrate` — no external migration tool needed.
