CREATE TABLE IF NOT EXISTS base.wallet_enrichment_sync_state
(
    source          LowCardinality(String),
    chain_id        UInt64,
    wallet_address  String,
    period          LowCardinality(String),
    status          LowCardinality(String),
    last_error      String,
    attempt_count   UInt32,
    attempted_at    DateTime64(3, 'UTC'),
    next_retry_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(attempted_at)
ORDER BY (source, chain_id, wallet_address, period);
