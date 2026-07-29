# Phase 4: AVE market data enrichment

The `market-sync` service enriches registry tokens without making AVE part of
the on-chain fact path. AVE is accessed through the replaceable
`marketdata.Provider` interface.

## Data flow

```text
PostgreSQL token registry
  -> market-sync
  -> AVE batch price API
  -> ClickHouse token_market_snapshots
  -> rotating AVE contract risk requests
  -> ClickHouse token_risk_snapshots
```

The batch price endpoint supplies:

- USD price and 24-hour price change;
- TVL and 24-hour USD volume;
- market capitalization and fully diluted valuation;
- holder count.

Contract risk snapshots retain selected normalized flags and the full raw JSON
report. The raw report allows new risk indicators to be extracted later
without refetching historical responses.

## Configuration

The API key belongs only in the ignored local `.env` or a production secret
manager. It must never be committed.

```dotenv
AVE_BASE_URL=https://prod.ave-api.com
AVE_API_KEY=
AVE_REQUEST_TIMEOUT=15s
AVE_MIN_REQUEST_INTERVAL=250ms
MARKET_SYNC_BATCH_SIZE=200
MARKET_RISK_BATCH_SIZE=10
MARKET_SYNC_INTERVAL=5m
```

Market batches can contain at most 200 tokens. Risk reports use a separate
rotating cursor; the default checks 10 tokens per cycle to conserve API quota.

## Operations

```shell
docker compose up -d market-sync
docker compose logs -f market-sync
```

Latest market state:

```sql
SELECT
    token_address,
    argMax(price_usd_raw, fetched_at) AS price_usd,
    argMax(tvl_usd_raw, fetched_at) AS tvl_usd,
    argMax(market_cap_usd_raw, fetched_at) AS market_cap_usd,
    argMax(holders, fetched_at) AS holders
FROM base.token_market_snapshots
WHERE chain_id = 8453 AND source = 'ave'
GROUP BY token_address;
```

Latest normalized risk state:

```sql
SELECT
    token_address,
    argMax(risk_score_raw, fetched_at) AS risk_score,
    argMax(is_honeypot, fetched_at) AS is_honeypot,
    argMax(has_mint_method, fetched_at) AS has_mint_method,
    argMax(is_proxy, fetched_at) AS is_proxy
FROM base.token_risk_snapshots
WHERE chain_id = 8453 AND source = 'ave'
GROUP BY token_address;
```
