CREATE TABLE IF NOT EXISTS base.dex_swap_valuations
(
    event_id                 String,
    chain_id                 UInt64,
    block_number             UInt64,
    block_time               DateTime64(3, 'UTC'),
    transaction_hash         String,
    transaction_index        UInt32,
    log_index                UInt32,
    pool_address             String,
    protocol                 LowCardinality(String),
    protocol_version         LowCardinality(String),
    token0_address           String,
    token1_address           String,
    token0_symbol            LowCardinality(String),
    token1_symbol            LowCardinality(String),
    token0_amount_raw        String,
    token1_amount_raw        String,
    token0_price_usd_raw     String,
    token1_price_usd_raw     String,
    token0_value_usd_raw     String,
    token1_value_usd_raw     String,
    trade_value_usd_raw      String,
    bought_token_address     String,
    bought_token_symbol      LowCardinality(String),
    sold_token_address       String,
    sold_token_symbol        LowCardinality(String),
    valuation_status         LowCardinality(String),
    is_large_trade           UInt8,
    price_source             LowCardinality(String),
    token0_price_updated_at  DateTime64(3, 'UTC'),
    token1_price_updated_at  DateTime64(3, 'UTC'),
    swap_observed_at         DateTime64(3, 'UTC'),
    valued_at                DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(valued_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_number, transaction_index, log_index, event_id);
