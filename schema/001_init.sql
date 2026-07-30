-- Anchor durable agent runtime: initial schema.
-- Applied idempotently by `anchor migrate`.
CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    workflow TEXT NOT NULL,
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL,
    event_seq BIGINT NOT NULL DEFAULT 0,
    attempt INT NOT NULL DEFAULT 0,
    budget JSONB NOT NULL DEFAULT '{}'::jsonb,
    usage JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    error JSONB,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
-- Submission-side exactly-once: a client that retries POST /v1/runs with the
-- same key gets the original run back instead of a duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS runs_idempotency_key_uniq ON runs (idempotency_key);
CREATE INDEX IF NOT EXISTS runs_status_idx ON runs (status, updated_at DESC);
-- Append-only history. The (run_id, seq) primary key is a correctness tripwire:
-- two workers writing the same run is a bug, and it fails loudly here.
CREATE TABLE IF NOT EXISTS events (
    run_id UUID NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    type TEXT NOT NULL,
    step_key TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS events_step_idx ON events (run_id, step_key);
-- Work queue. Claimed with FOR UPDATE SKIP LOCKED, held by a time-based lease.
-- A worker that dies stops heartbeating, the lease expires, another worker
-- claims the row and replays the run from the event log.
CREATE TABLE IF NOT EXISTS run_queue (
    run_id UUID PRIMARY KEY REFERENCES runs (run_id) ON DELETE CASCADE,
    visible_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    worker_id TEXT,
    attempts INT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS run_queue_claimable_idx ON run_queue (visible_at, locked_until);
-- Stand-in for a real external system (a payment processor, an email provider).
-- The UNIQUE constraint on token is what makes "exactly once" a measurable
-- claim instead of a marketing one: a replayed side effect collides here.
CREATE TABLE IF NOT EXISTS side_effects (
    token UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS side_effects_run_idx ON side_effects (run_id, kind);