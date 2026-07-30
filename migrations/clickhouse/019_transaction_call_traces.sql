CREATE TABLE IF NOT EXISTS base.transaction_call_traces
(
    trace_id             String,
    trace_version        LowCardinality(String),
    chain_id             UInt64,
    block_number         UInt64,
    block_hash           LowCardinality(String),
    block_time           DateTime64(3, 'UTC'),
    transaction_hash     String,
    transaction_index    UInt32,
    trace_address        Array(UInt32),
    parent_trace_address Array(UInt32),
    depth                UInt32,
    call_type            LowCardinality(String),
    from_address         String,
    to_address           String,
    value_raw            String,
    gas_raw              String,
    gas_used_raw         String,
    input                String,
    output               String,
    function_selector    LowCardinality(String),
    function_name        LowCardinality(String),
    error                String,
    revert_reason        String,
    emitted_log_count    UInt32,
    success              UInt8,
    is_pool_call         UInt8,
    is_router_call       UInt8,
    is_multicall         UInt8,
    traced_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(traced_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, transaction_index, transaction_hash, trace_address);
