CREATE TABLE IF NOT EXISTS base.erc20_transfers
(
    event_id           String,
    schema_version     LowCardinality(String),
    chain_id           UInt64,
    block_number       UInt64,
    block_hash         LowCardinality(String),
    block_time         DateTime64(3, 'UTC'),
    transaction_hash   String,
    transaction_index  UInt32,
    log_index          UInt32,
    token_address      LowCardinality(String),
    from_address       LowCardinality(String),
    to_address         LowCardinality(String),
    amount_raw         String,
    parser_version     LowCardinality(String),
    observed_at        DateTime64(3, 'UTC'),
    is_canonical       UInt8
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, transaction_index, log_index, event_id);
