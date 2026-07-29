CREATE TABLE IF NOT EXISTS base.raw_blocks
(
    chain_id           UInt64,
    block_number       UInt64,
    block_hash         LowCardinality(String),
    parent_hash        String,
    block_time         DateTime64(3, 'UTC'),
    transactions_root  String,
    receipts_root      String,
    state_root         String,
    gas_used           UInt64,
    gas_limit          UInt64,
    transaction_count  UInt32,
    receipt_count      UInt32,
    provider           LowCardinality(String),
    observed_at        DateTime64(3, 'UTC'),
    schema_version     LowCardinality(String),
    is_canonical       UInt8,
    raw_json           String
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, block_hash);
