CREATE TABLE IF NOT EXISTS base.wallet_swap_activities
(
    event_id                  String,
    valuation_version        LowCardinality(String),
    chain_id                 UInt64,
    wallet_address           String,
    router_address           String,
    attribution_method       LowCardinality(String),
    block_number             UInt64,
    block_time               DateTime64(3, 'UTC'),
    transaction_hash         String,
    transaction_index        UInt32,
    log_index                UInt32,
    pool_address             String,
    protocol                 LowCardinality(String),
    protocol_version         LowCardinality(String),
    bought_token_address     String,
    bought_token_symbol      LowCardinality(String),
    bought_token_amount_raw  String,
    sold_token_address       String,
    sold_token_symbol        LowCardinality(String),
    sold_token_amount_raw    String,
    trade_value_usd_raw      String,
    valuation_status         LowCardinality(String),
    source_valued_at         DateTime64(3, 'UTC'),
    generated_at             DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generated_at)
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, wallet_address, block_number, transaction_index, log_index, event_id);
