CREATE TABLE IF NOT EXISTS base.token_risk_snapshots
(
    chain_id              UInt64,
    token_address         String,
    source                LowCardinality(String),
    risk_score_raw        String,
    is_honeypot           Nullable(UInt8),
    has_mint_method       Nullable(UInt8),
    has_black_method      Nullable(UInt8),
    is_proxy              Nullable(UInt8),
    owner_address         String,
    buy_tax_raw           String,
    sell_tax_raw          String,
    raw_json              String,
    source_updated_at     DateTime64(3, 'UTC'),
    fetched_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(source_updated_at)
ORDER BY (chain_id, token_address, source, source_updated_at);
