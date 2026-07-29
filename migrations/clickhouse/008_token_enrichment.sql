CREATE TABLE IF NOT EXISTS base.token_market_snapshots
(
    chain_id              UInt64,
    token_address         String,
    source                LowCardinality(String),
    price_usd_raw         String,
    price_change_24h_raw  String,
    tvl_usd_raw           String,
    market_cap_usd_raw    String,
    fdv_usd_raw           String,
    volume_24h_usd_raw    String,
    holders               UInt64,
    source_updated_at     DateTime64(3, 'UTC'),
    fetched_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(source_updated_at)
ORDER BY (chain_id, token_address, source, source_updated_at);
