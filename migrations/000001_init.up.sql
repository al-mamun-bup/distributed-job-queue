CREATE TYPE job_state AS ENUM ('pending', 'running', 'succeeded', 'dead');

CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    queue text NOT NULL,
    payload jsonb NOT NULL,
    state job_state NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5,
    run_at timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz NULL,
    worker_id text NULL,
    idempotency_key text NULL,
    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz NULL
);

CREATE INDEX idx_jobs_claim ON jobs (queue, state, run_at);
CREATE INDEX idx_jobs_reaper_running_lease ON jobs (lease_expires_at) WHERE state = 'running';
CREATE UNIQUE INDEX idx_jobs_queue_idempotency_key_unique
ON jobs (queue, idempotency_key)
WHERE idempotency_key IS NOT NULL;
