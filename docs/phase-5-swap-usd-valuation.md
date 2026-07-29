# Phase 5: swap USD valuation

The `valuation-worker` converts pool-relative raw swap deltas into normalized
token amounts and conservative USD trade values.

## Inputs

- normalized swaps from `base.dex_pool_swaps`;
- token decimals from the on-chain registry enrichment;
- latest AVE prices from `base.token_market_snapshots`.

Price freshness is measured between `swap.observed_at` and the local AVE
`fetched_at`. This is a processing-time valuation. It must not be interpreted
as a historical candle price for old blocks.

## Valuation rules

- Pool delta greater than zero means the trader sold that token into the pool.
- Pool delta less than zero means the trader bought that token from the pool.
- Raw integer amounts are normalized with exact arbitrary-precision rational
  arithmetic.
- When both token prices are fresh and their USD values agree within 3x, the
  trade value is the mean of both sides.
- If only one price is available, the row is `single_sided` and cannot trigger
  a large-trade alert.
- If both prices exist but imply values differing by more than 3x, the row is
  `price_mismatch`. The smaller value is retained as a conservative estimate
  and large-trade alerts are suppressed.

The current algorithm version is `usd-v2`. Consumers should query
`base.dex_swap_valuations_current`, which excludes obsolete algorithm versions.

## Configuration

```dotenv
VALUATION_BATCH_SIZE=500
VALUATION_LOOKBACK=1h
VALUATION_MAX_PRICE_AGE=10m
VALUATION_POLL_INTERVAL=2s
LARGE_TRADE_USD=10000
```

## Operations

```shell
docker compose up -d valuation-worker
docker compose logs -f valuation-worker
```

Large trades:

```sql
SELECT
    block_time,
    transaction_hash,
    protocol,
    bought_token_symbol,
    sold_token_symbol,
    trade_value_usd_raw
FROM base.dex_swap_valuations_current
WHERE is_large_trade = 1
ORDER BY valued_at DESC;
```

Data-quality monitoring:

```sql
SELECT valuation_status, count()
FROM base.dex_swap_valuations_current
GROUP BY valuation_status;
```
