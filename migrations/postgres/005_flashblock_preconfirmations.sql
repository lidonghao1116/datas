CREATE TABLE IF NOT EXISTS flashblock_preconfirmations (
    preconfirmation_key TEXT PRIMARY KEY,
    chain_id             BIGINT NOT NULL,
    transaction_hash    TEXT NOT NULL,
    log_index            INTEGER NOT NULL,
    pool_address         TEXT NOT NULL,
    preconfirmed_block   BIGINT NOT NULL,
    preconfirmed_hash    TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'pending',
    alert_keys           TEXT[] NOT NULL DEFAULT '{}',
    payload              JSONB NOT NULL,
    observed_at          TIMESTAMPTZ NOT NULL,
    expires_at           TIMESTAMPTZ NOT NULL,
    next_check_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt_count        INTEGER NOT NULL DEFAULT 0,
    last_error           TEXT NOT NULL DEFAULT '',
    canonical_block      BIGINT,
    canonical_block_hash TEXT,
    resolved_at          TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT flashblock_preconfirmations_status_check
        CHECK (status IN ('pending', 'confirmed', 'reverted', 'expired'))
);

CREATE INDEX IF NOT EXISTS flashblock_preconfirmations_pending_idx
    ON flashblock_preconfirmations (next_check_at, observed_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS flashblock_preconfirmations_transaction_idx
    ON flashblock_preconfirmations (transaction_hash, log_index);
