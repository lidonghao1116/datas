CREATE TABLE IF NOT EXISTS base.wallet_smart_score_snapshots
(
    chain_id                       UInt64,
    wallet_address                 String,
    analytics_version              LowCardinality(String),
    realized_profit_usd_raw        String,
    unrealized_profit_usd_raw      String,
    total_profit_usd_raw           String,
    total_invested_usd_raw         String,
    roi_raw                        String,
    win_rate_raw                   String,
    smart_score_raw                String,
    smart_score_grade              LowCardinality(String),
    performance_score_raw          String,
    win_rate_score_raw             String,
    track_record_score_raw         String,
    activity_score_raw             String,
    risk_score_raw                 String,
    confidence_raw                 String,
    trade_count                    UInt64,
    closed_sell_count              UInt64,
    winning_sell_count             UInt64,
    active_days                    UInt64,
    unique_non_quote_tokens        UInt64,
    risky_token_count              UInt64,
    unmatched_sell_count           UInt64,
    missing_price_position_count   UInt64,
    partial_valuation_count        UInt64,
    transfer_in_count              UInt64,
    transfer_out_count             UInt64,
    history_incomplete             UInt8,
    source_updated_at              DateTime64(3, 'UTC'),
    calculated_at                  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(calculated_at)
ORDER BY (chain_id, wallet_address, analytics_version, calculated_at);
