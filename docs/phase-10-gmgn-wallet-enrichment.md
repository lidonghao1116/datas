# Phase 10: GMGN wallet enrichment

This phase adds GMGN as a replaceable wallet-data provider. The local
`wallet_swap_activities` facts remain the primary source; GMGN snapshots are
optional enrichment and never block on-chain ingestion.

Official references:

- https://docs.gmgn.ai/index/gmgn-agent-api
- https://github.com/GMGNAI/gmgn-skills
- https://github.com/GMGNAI/gmgn-skills/blob/main/skills/gmgn-portfolio/SKILL.md

## Scope and security

The service calls only the read-only portfolio statistics endpoint:

```http
GET https://openapi.gmgn.ai/v1/user/wallet_stats
X-APIKEY: ${GMGN_API_KEY}
```

No GMGN private key is accepted, stored, or used. Trading, holdings requiring
signed authentication, and order submission are outside this service.

## Pipeline

```text
recent wallet_swap_activities
        |
        v
gmgn-wallet-sync
        |
        +--> GMGN portfolio stats (7d / 30d)
        |
        +--> gmgn_wallet_profile_snapshots
        |
        +--> wallet_enrichment_sync_state
        |
        v
GET /api/v1/wallets/{address}/profile
```

Candidate selection prioritizes recently active wallets whose snapshot is
missing or expired. A failed request records a retry time, preventing a broken
or rate-limited upstream from entering a tight retry loop.

## Stored enrichment

Each period snapshot stores:

- realized and unrealized profit;
- PnL ratio and win rate;
- total cost, buy count, and sell count;
- token count and average holding duration;
- GMGN tags and primary label;
- name, ENS, and Twitter identity where available;
- wallet age and funding-source metadata where available;
- source timestamp, fetch timestamp, expiry timestamp;
- the complete raw upstream JSON.

The normalized fields are derived from the documented response and the current
Base response shape. Raw JSON is retained because GMGN can add or rename
fields independently of this application.

## API response

The self-developed profile remains at the top level. GMGN appears as an
optional nested object:

```json
{
  "data": {
    "wallet_address": "0x...",
    "swap_count": 42,
    "gmgn": {
      "source": "gmgn",
      "available": true,
      "identity": {
        "primary_tag": "smart_money",
        "tags": ["smart_money"]
      },
      "periods": {
        "7d": {
          "realized_profit_raw": "120.5",
          "win_rate_raw": "0.6",
          "is_stale": false
        }
      },
      "sync": {
        "7d": {
          "status": "success"
        }
      }
    }
  }
}
```

If GMGN is unavailable, the local profile still returns normally. Existing
snapshots are returned with `is_stale=true` after expiry.

## Configuration

```dotenv
GMGN_BASE_URL=https://openapi.gmgn.ai
GMGN_API_KEY=
GMGN_REQUEST_TIMEOUT=15s
GMGN_MIN_REQUEST_INTERVAL=5s
GMGN_WALLET_PERIODS=7d,30d
GMGN_WALLET_SYNC_BATCH_SIZE=5
GMGN_WALLET_FRESHNESS=1h
GMGN_WALLET_ACTIVE_LOOKBACK=24h
GMGN_WALLET_SYNC_INTERVAL=5s
GMGN_WALLET_RETRY_BASE=5m
```

`GMGN_API_KEY` belongs only in the ignored local `.env` or a production secret
manager. When it is empty, the sync service stays disabled without affecting
the rest of the pipeline.

## Run

```powershell
docker compose run --rm migrate
docker compose up -d --build gmgn-wallet-sync query-api
docker compose logs -f gmgn-wallet-sync
```
