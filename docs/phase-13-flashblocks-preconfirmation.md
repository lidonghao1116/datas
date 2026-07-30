# Phase 13: Flashblocks preconfirmation alerts

This phase adds a preconfirmation lane beside the canonical Base ingestion
pipeline. It reduces detection latency without treating sequencer
preconfirmations as finalized facts.

Official references:

- https://docs.base.org/base-chain/api-reference/flashblocks-api/pendingLogs
- https://docs.base.org/base-chain/api-reference/flashblocks-api/flashblocks-api-overview
- https://docs.base.org/base-chain/api-reference/ethereum-json-rpc-api/eth_getLogs

## Sources and fallback

The worker first attempts the application-facing Flashblocks WebSocket:

```text
wss://mainnet-preconf.base.org
eth_subscribe("pendingLogs", {topics: [[V2 Swap, V3 Swap]]})
```

`pendingLogs` normally emits approximately every 200 ms. The raw
`wss://mainnet.flashblocks.base.org/ws` infrastructure stream is deliberately
not used; Base reserves it for node operators.

If the configured WSS endpoint rejects or drops the subscription, the worker
falls back to a single batched HTTP request containing:

```text
eth_getLogs({fromBlock: "pending", toBlock: "pending", topics: [...]})
eth_getBlockByNumber("pending", true)
```

The first result supplies Swap logs and the second supplies transaction senders
without one RPC call per transaction. The public endpoint fallback defaults to
two-second polling to respect rate limits. Matching logs are processed with a
bounded concurrency of eight so a busy pending block cannot consume the WSS
reconnect window. A production provider that supports `pendingLogs` retains the
approximately 200 ms path.

## Preconfirmation processing

For each V2 or V3 Swap log:

1. Decode the log with the same parser used by the canonical pipeline.
2. Attribute the wallet from the preconfirmed transaction `from` field.
3. Resolve known pool and token metadata from local ClickHouse facts.
4. Use fresh AVE prices with the existing `usd-v2` valuation rules.
5. Apply the existing large-trade and `smart-v1` smart-money gates.
6. Emit `preconfirmed_large_*` and/or
   `preconfirmed_smart_money_*` through the PostgreSQL Outbox.

Unknown pools, stale prices, price mismatches, and signals below configured
thresholds do not generate speculative alerts. The canonical pipeline remains
responsible for complete eventual processing.

## State and reconciliation

Preconfirmation lifecycle is stored in `flashblock_preconfirmations`,
independently from Outbox delivery state:

```text
pending -> confirmed
pending -> reverted
pending -> expired
```

The key is `(chain_id, transaction_hash, log_index)`. Initial state and initial
Outbox alerts are inserted in one PostgreSQL transaction.

The reconciler queries `eth_getTransactionReceipt` through the standard Base
RPC, not the Flashblocks endpoint:

- receipt succeeds and contains the matching pool/log index: `confirmed`;
- failed receipt or missing matching log: `reverted`;
- no canonical receipt before the configured TTL: `expired`;
- a `removed: true` pending log: immediate `reverted`.

Every transition emits one of:

```text
preconfirmation_confirmed
preconfirmation_reverted
preconfirmation_expired
```

Consumers can therefore act on a low-latency signal and later reconcile their
state. A confirmed transition does not replace the canonical `large_*` or
`smart_money_*` alert.

## Configuration

```dotenv
FLASHBLOCKS_HTTP_URL=https://mainnet-preconf.base.org
FLASHBLOCKS_WSS_URL=wss://mainnet-preconf.base.org
FLASHBLOCKS_RECONCILIATION_TTL=30s
FLASHBLOCKS_RECONCILE_BATCH=100
FLASHBLOCKS_RECONCILE_INTERVAL=1s
FLASHBLOCKS_RECONNECT_DELAY=30s
FLASHBLOCKS_REQUEST_TIMEOUT=2s
FLASHBLOCKS_FALLBACK_POLL_INTERVAL=2s
```

Provider URLs should be replaced with a production Flashblocks-aware RPC
provider because Base public endpoints are explicitly rate limited.

## Operations

```shell
docker compose up -d flashblocks-worker
docker compose logs -f flashblocks-worker
```

Lifecycle summary:

```sql
SELECT status, count(*)
FROM flashblock_preconfirmations
GROUP BY status;
```

Alert API examples:

```http
GET /api/v1/alerts?type=preconfirmed_large_buy
GET /api/v1/alerts?type=preconfirmed_smart_money_buy
GET /api/v1/alerts?type=preconfirmation_reverted
```
