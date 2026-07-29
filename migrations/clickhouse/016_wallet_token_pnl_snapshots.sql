CREATE TABLE IF NOT EXISTS base.wallet_token_pnl_snapshots
(
    chain_id                  UInt64,
    wallet_address            String,
    token_address             String,
    token_symbol              LowCardinality(String),
    analytics_version         LowCardinality(String),
    is_quote_token            UInt8,
    bought_amount_raw         String,
    sold_amount_raw           String,
    remaining_amount_raw      String,
    total_buy_cost_usd_raw    String,
    total_sell_income_usd_raw String,
    remaining_cost_usd_raw    String,
    realized_profit_usd_raw   String,
    unrealized_profit_usd_raw String,
    total_profit_usd_raw      String,
    current_value_usd_raw     String,
    average_cost_usd_raw      String,
    current_price_usd_raw     String,
    buy_count                 UInt64,
    sell_count                UInt64,
    winning_sell_count        UInt64,
    unmatched_sell_amount_raw String,
    unmatched_sell_usd_raw    String,
    is_honeypot               UInt8,
    has_mint_method           UInt8,
    has_black_method          UInt8,
    is_proxy                  UInt8,
    data_quality              LowCardinality(String),
    first_traded_at           DateTime64(3, 'UTC'),
    last_traded_at            DateTime64(3, 'UTC'),
    price_updated_at          DateTime64(3, 'UTC'),
    source_updated_at         DateTime64(3, 'UTC'),
    calculated_at             DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(calculated_at)
ORDER BY (
    chain_id,
    wallet_address,
    token_address,
    analytics_version,
    calculated_at
);
