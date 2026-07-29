CREATE TABLE IF NOT EXISTS base.gmgn_wallet_profile_snapshots
(
    chain_id                UInt64,
    wallet_address          String,
    period                  LowCardinality(String),
    source                  LowCardinality(String),
    native_balance_raw      String,
    realized_profit_raw     String,
    unrealized_profit_raw   String,
    pnl_raw                 String,
    win_rate_raw            String,
    total_cost_raw          String,
    buy_count               UInt64,
    sell_count              UInt64,
    token_count             UInt64,
    avg_holding_seconds     UInt64,
    display_name            String,
    ens                     String,
    primary_tag             String,
    tags                    Array(String),
    twitter_username        String,
    twitter_name            String,
    twitter_followers       UInt64,
    is_blue_verified        UInt8,
    created_token_count     UInt64,
    wallet_created_at       DateTime64(3, 'UTC'),
    fund_from               String,
    fund_from_address       String,
    fund_amount_raw         String,
    source_updated_at       DateTime64(3, 'UTC'),
    fetched_at              DateTime64(3, 'UTC'),
    expires_at              DateTime64(3, 'UTC'),
    raw_json                String
)
ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (chain_id, wallet_address, period, fetched_at);
