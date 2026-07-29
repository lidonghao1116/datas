CREATE TABLE IF NOT EXISTS base.dex_pool_swaps
(
    event_id            String,
    schema_version      LowCardinality(String),
    chain_id            UInt64,
    block_number        UInt64,
    block_hash          LowCardinality(String),
    block_time          DateTime64(3, 'UTC'),
    transaction_hash    String,
    transaction_index   UInt32,
    log_index           UInt32,
    pool_address        LowCardinality(String),
    protocol_family     LowCardinality(String),
    sender_address      LowCardinality(String),
    recipient_address   LowCardinality(String),
    amount0_delta_raw   String,
    amount1_delta_raw   String,
    sqrt_price_x96_raw  String,
    liquidity_raw       String,
    tick                Int32,
    parser_version      LowCardinality(String),
    observed_at         DateTime64(3, 'UTC'),
    is_canonical        UInt8
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, transaction_index, log_index, event_id);
