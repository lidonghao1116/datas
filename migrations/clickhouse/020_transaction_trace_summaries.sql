CREATE TABLE IF NOT EXISTS base.transaction_trace_summaries
(
    trace_version       LowCardinality(String),
    chain_id           UInt64,
    block_number       UInt64,
    block_hash         LowCardinality(String),
    block_time         DateTime64(3, 'UTC'),
    transaction_hash   String,
    transaction_index  UInt32,
    wallet_address     String,
    transaction_target String,
    root_selector      LowCardinality(String),
    root_function      LowCardinality(String),
    frame_count        UInt32,
    max_depth          UInt32,
    failed_call_count  UInt32,
    delegatecall_count UInt32,
    pool_call_count    UInt32,
    router_addresses   Array(String),
    multicall_selectors Array(String),
    raw_trace_json     String,
    traced_at          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(traced_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, transaction_index, transaction_hash);
