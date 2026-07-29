CREATE TABLE IF NOT EXISTS base.transaction_trace_sync_state
(
    trace_version      LowCardinality(String),
    chain_id           UInt64,
    block_number       UInt64,
    transaction_hash  String,
    status             LowCardinality(String),
    attempt_count      UInt32,
    last_error         String,
    next_retry_at      DateTime64(3, 'UTC'),
    attempted_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(attempted_at)
PARTITION BY toYYYYMM(attempted_at)
ORDER BY (trace_version, chain_id, transaction_hash);
