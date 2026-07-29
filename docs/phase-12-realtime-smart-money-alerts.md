# Phase 12: real-time smart-money and large-trade alerts

This phase joins confirmed Base swap activity with the latest local
`smart-v1` wallet score and emits independently subscribable large-trade and
smart-money alerts through the existing transactional Outbox.

## Pipeline

```text
Base swap valuation
        |
        v
wallet_swap_activities ---- wallet_smart_score_snapshots
        |                              |
        +--------------+---------------+
                       v
                  alert-worker
                       |
                 PostgreSQL Outbox
                  /            \
                 v              v
          alert-dispatcher   query-api / WebSocket
```

Base RPC and locally observed swaps remain the facts. AVE risk data can raise
severity, while GMGN does not control whether a wallet is considered smart.

## Alert types

Direction is inferred with `ALERT_QUOTE_SYMBOLS`:

| Condition | Types |
| --- | --- |
| Non-quote token bought with quote asset | `large_buy`, `smart_money_buy` |
| Non-quote token sold for quote asset | `large_sell`, `smart_money_sell` |
| Other pair shape | `large_swap`, `smart_money_swap` |

A transaction can create both a large-trade alert and a smart-money alert. The
two records have different deterministic keys, so consumers can subscribe to
one category without losing the other.

## Smart-money gate

A smart-money alert requires all of the following:

```text
score version == ALERT_SMART_SCORE_VERSION
smart score >= ALERT_SMART_SCORE_MIN
confidence >= ALERT_SMART_CONFIDENCE_MIN
trade value USD >= ALERT_SMART_TRADE_MIN_USD
```

Default values require a `smart-v1` B-grade wallet, confidence of at least 0.6,
and a trade worth at least $1,000. These gates intentionally prevent a high
score derived from a tiny or low-quality sample from producing alerts.

The alert payload includes:

- wallet address and attribution method;
- trade, pool, protocol, token, value, and block fields;
- target-token AVE risk flags;
- score version, score, grade, confidence, source timestamp, and calculation
  timestamp;
- whether the same trade also met the large-trade threshold.

## Severity

- `critical`: the target token is marked as a honeypot or has a blacklist
  method;
- `high`: trade value reaches `ALERT_CRITICAL_USD`, the token is mintable or
  proxy-based, or the alert is from a qualified smart-money wallet;
- `medium`: other large trades.

## Idempotency and restart behavior

The detector reads activity in `(generated_at, event_id)` order. It immediately
continues when a full batch is returned, and polls only after catching up.

Each Outbox key is deterministic:

```text
valuation_version:event_id:alert_type:target_token
```

On restart, the worker rescans only `ALERT_LOOKBACK`. PostgreSQL
`ON CONFLICT DO NOTHING` suppresses duplicate delivery. The existing dispatcher
retains leasing, exponential retry, and dead-letter behavior.

## Configuration

```dotenv
ALERT_BATCH_SIZE=500
ALERT_LOOKBACK=1h
ALERT_POLL_INTERVAL=5s
ALERT_CRITICAL_USD=50000
ALERT_QUOTE_SYMBOLS=WETH,USDC,USDbC,USDT,DAI,EURC,cbBTC
ALERT_SMART_SCORE_VERSION=smart-v1
ALERT_SMART_SCORE_MIN=65
ALERT_SMART_CONFIDENCE_MIN=0.6
ALERT_SMART_TRADE_MIN_USD=1000
```

## Consumption

Query one alert type:

```http
GET /api/v1/alerts?type=smart_money_buy&limit=50
GET /api/v1/alerts?type=large_sell&severity=critical&limit=50
```

All new Outbox rows are also broadcast through:

```text
ws://localhost:8080/ws/alerts
```

When `ALERT_WEBHOOK_URL` is configured, the dispatcher sends the same JSON
records with the existing HMAC signature.

## Current real-time boundary

This implementation reacts to confirmed and valued swap activity, normally
within the wallet-profile and alert polling delay. It is not yet the 200 ms
Flashblocks path. A later `pendingLogs` adapter can feed the same alert model,
but pending alerts must be distinguished from confirmed alerts and reconciled
after reorgs.
