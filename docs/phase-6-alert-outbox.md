# Phase 6: alert engine and outbox

The `alert-worker` converts reliable `usd-v2` large-trade valuations into
structured alerts and stores them in a PostgreSQL transactional outbox.

## Classification

The engine uses configurable quote symbols to identify the non-quote target:

- trader buys a non-quote token with a quote asset: `large_buy`;
- trader sells a non-quote token for a quote asset: `large_sell`;
- both sides are quote assets or both sides are non-quote assets: `large_swap`.

Default quote symbols:

```text
WETH, USDC, USDbC, USDT, DAI, EURC, cbBTC
```

Symbol-based classification is an initial routing heuristic. Address-level
asset classification should replace it when the curated asset registry is
introduced.

## Severity

- `critical`: target token is marked as a honeypot or has a blacklist method;
- `high`: trade value exceeds `ALERT_CRITICAL_USD`, or the target is mintable
  or proxy-based;
- `medium`: other reliable large trades.

Only valuations where `is_large_trade = 1` are eligible. `single_sided` and
`price_mismatch` valuations cannot set this flag.

## Delivery guarantees

`alert_key` is deterministic across repeated scans:

```text
valuation_version:event_id:alert_type:target_token
```

`INSERT ... ON CONFLICT DO NOTHING` makes detection idempotent. A
`(valued_at,event_id)` cursor prevents batch starvation at high event rates.
Rows remain `pending` until a delivery worker sends them to a configured
channel.

## Configuration

```dotenv
ALERT_BATCH_SIZE=500
ALERT_LOOKBACK=1h
ALERT_POLL_INTERVAL=5s
ALERT_CRITICAL_USD=50000
ALERT_QUOTE_SYMBOLS=WETH,USDC,USDbC,USDT,DAI,EURC,cbBTC
```

## Operations

```shell
docker compose up -d alert-worker
docker compose logs -f alert-worker
```

Pending alerts:

```sql
SELECT alert_type, severity, count(*)
FROM alert_outbox
WHERE status = 'pending'
GROUP BY alert_type, severity;
```

Latest alerts:

```sql
SELECT created_at, alert_type, severity, token_symbol, title
FROM alert_outbox
ORDER BY created_at DESC
LIMIT 100;
```
