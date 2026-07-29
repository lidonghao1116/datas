CREATE TABLE IF NOT EXISTS alert_outbox (
    alert_key          TEXT PRIMARY KEY,
    alert_type         TEXT NOT NULL,
    severity           TEXT NOT NULL,
    chain_id           BIGINT NOT NULL,
    block_number       BIGINT NOT NULL,
    transaction_hash   TEXT NOT NULL,
    token_address      TEXT NOT NULL DEFAULT '',
    token_symbol       TEXT NOT NULL DEFAULT '',
    title              TEXT NOT NULL,
    payload             JSONB NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    attempts            INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at        TIMESTAMPTZ,
    CONSTRAINT alert_outbox_status_check
        CHECK (status IN ('pending', 'processing', 'delivered', 'failed'))
);

CREATE INDEX IF NOT EXISTS alert_outbox_pending_idx
    ON alert_outbox (next_attempt_at, created_at)
    WHERE status IN ('pending', 'failed');
