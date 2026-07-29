# Phase 11: self-developed wallet PnL and smart-money score

This phase calculates wallet performance from locally observed Base swap facts.
AVE supplies replaceable price and risk inputs, while GMGN remains an optional
comparison and enrichment source. Neither provider is the source of the
self-developed score.

## Data flow

```text
wallet_swap_activities + wallet_transfer_activities
                         |
                         v
                wallet-analytics-worker
                         |
             +-----------+-----------+
             |                       |
             v                       v
wallet_token_pnl_snapshots  wallet_smart_score_snapshots
             |                       |
             +-----------+-----------+
                         v
                    query-api
```

The worker selects a wallet when its newest activity timestamp in milliseconds
is newer than the source cursor stored with its latest `smart-v1` score. This
makes recalculation incremental and also avoids repeatedly selecting multiple
events that happened within the same second.

## PnL accounting

Token inventory uses moving weighted-average cost:

```text
average cost = remaining cost / remaining quantity
realized PnL = covered sale proceeds - average cost before sale * covered quantity
unrealized PnL = current quantity * fresh market price - remaining cost
```

Every swap contributes one buy leg and one sell leg. A sell larger than the
locally observed inventory is split: only the covered quantity affects realized
PnL, while the rest is recorded as an unmatched sell. It is not treated as
zero-cost profit.

The wallet aggregate excludes configured quote assets such as WETH, USDC,
USDbC, USDT, DAI, EURC, and cbBTC. Their token-level snapshots are still
available. An open position receives unrealized PnL only when its AVE price is
present and no older than `WALLET_ANALYTICS_MAX_PRICE_AGE`.

Each token snapshot exposes one of these quality states:

- `complete`: locally observed inventory and required current price are usable;
- `incomplete_history`: a sale predates or exceeds the observed inventory;
- `missing_price`: an open position has no sufficiently fresh price.

## `smart-v1` score

The score is versioned so future formula changes can coexist with historical
results. Its base score is the sum of:

| Component | Maximum | Signal |
| --- | ---: | --- |
| Performance | 35 | ROI, normalized from -50% to +200% |
| Win rate | 25 | Profitable covered sells / covered sells |
| Track record | 15 | Reaches full weight at 20 covered sells |
| Activity | 15 | Active days and distinct non-quote tokens |
| Risk | 10 | Share of tokens without AVE risk flags |

The final score applies a data-confidence adjustment. Confidence starts from
the local trade sample size and is reduced for direct transfers, unmatched
sells, partially valued swaps, and missing current prices. The adjustment keeps
30% of the confidence effect:

```text
smart score = base score * (0.7 + 0.3 * confidence)
```

Grades are `A` (80+), `B` (65+), `C` (50+), `D` (35+), and `E` (below 35).
The API returns every component, the confidence value, and the data-quality
counters so consumers can audit the result instead of relying on the grade
alone.

## API

```http
GET /api/v1/wallets/{address}/score
GET /api/v1/wallets/{address}/pnl?limit=50
GET /api/v1/wallets/{address}/profile
```

`/score` returns the latest `smart-v1` aggregate. `/pnl` returns the latest
token snapshots, ordered by total profit, with a maximum limit enforced by the
API. `/profile` embeds the same aggregate as `smart_score`; GMGN remains under
the independent `gmgn` field.

## Configuration

```dotenv
WALLET_ANALYTICS_BATCH_SIZE=20
WALLET_ANALYTICS_POLL_INTERVAL=5s
WALLET_ANALYTICS_MAX_PRICE_AGE=2h
ALERT_QUOTE_SYMBOLS=WETH,USDC,USDbC,USDT,DAI,EURC,cbBTC
```

## Current limitations

- Results cover the locally indexed history, not activity before ingestion
  began. Archive RPC backfill will improve this.
- Wallet attribution currently follows the observed swap initiator. Trace data
  is still required for exact Router, Multicall, proxy, and delegated-wallet
  attribution.
- Direct transfers change economic inventory but cannot yet be assigned a
  reliable USD cost basis, so they reduce confidence instead.
- Gas, token transfer taxes, rebases, airdrop cost basis, and off-chain trades
  are not included.
- AVE price and risk fields are replaceable inputs. Missing or stale inputs
  degrade data quality; they do not fabricate values.
