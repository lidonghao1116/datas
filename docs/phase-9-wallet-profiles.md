# Phase 9: Wallet profile foundation

The wallet profile pipeline converts valued DEX swaps into an idempotent,
queryable wallet activity layer.

## Attribution model

The current fact set identifies the transaction initiator from
`raw_transactions.from_address`. Each derived activity records:

```text
attribution_method = transaction_from
```

This is deterministic and available without an archive node. It attributes all
pool swaps in a Router or Multicall transaction to the transaction initiator.
It does not claim to identify an internal call's final beneficiary. Trace-based
attribution will supersede this method in the Archive RPC phase.

## Pipeline

```text
dex_swap_valuations_current
        +
raw_transactions
        |
        v
wallet-profile-worker
        |
        v
wallet_swap_activities
        |
        v
Query API profile / trades / positions
```

The worker compares each valuation's `valued_at` with the latest derived
activity's `source_valued_at`. It therefore supports:

- restart-safe historical backfill;
- idempotent retries;
- regeneration after a newer valuation;
- continuous incremental processing.

## Wallet APIs

```http
GET /api/v1/wallets/{address}/profile
GET /api/v1/wallets/{address}/trades?limit=50
GET /api/v1/wallets/{address}/positions?limit=50
```

The profile combines directly observed ERC-20 Transfer participation with
derived Swap activity. It includes:

- first and latest activity time;
- active days and unique transaction count;
- swap count and unique swap transaction count;
- quote-token buy, sell, and other-swap counts;
- directly observed transfer-in and transfer-out counts;
- aggregate swap USD volume;
- unique traded tokens;
- most frequently involved token and protocol.

Positions use normalized Swap token amounts:

```text
net_amount = observed bought amount - observed sold amount
position_basis = observed_swap_flow
```

They are behavioral flow estimates, not `balanceOf` results. Direct transfers,
rebasing tokens, mint/burn events, fees, and pre-existing balances can make them
different from the current on-chain balance.

## Configuration

```dotenv
WALLET_PROFILE_BATCH_SIZE=1000
WALLET_PROFILE_POLL_INTERVAL=2s
```

## Run and verify

```powershell
docker compose run --rm migrate
docker compose up -d --build wallet-profile-worker query-api

docker exec base-analytics-clickhouse-1 clickhouse-client --database base `
  --query "SELECT count(), uniqExact(wallet_address) FROM wallet_swap_activities FINAL"
```

The REST API returns `404` for an address with no observed Swap or Transfer
activity. Page limits default to 50 and are capped at 200.
