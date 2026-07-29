CREATE TABLE IF NOT EXISTS base.raw_transactions
(
    chain_id           UInt64,
    block_number       UInt64,
    block_hash         LowCardinality(String),
    block_time         DateTime64(3, 'UTC'),
    transaction_hash   String,
    transaction_index  UInt32,
    from_address       LowCardinality(String),
    to_address         LowCardinality(String),
    nonce              UInt64,
    value_raw          String,
    gas                UInt64,
    gas_price_raw      String,
    input              String,
    transaction_type   UInt8,
    observed_at        DateTime64(3, 'UTC'),
    is_canonical       UInt8,
    raw_json           String
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, transaction_index, transaction_hash);
