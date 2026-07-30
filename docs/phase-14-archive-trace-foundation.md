# Phase 14: Archive RPC transaction trace foundation

This phase adds a deliberately slow analysis lane for precise transaction call
trees. It is separate from canonical ingestion and Flashblocks alerting because
debug methods replay EVM execution and can be computationally expensive.

Official references:

- https://docs.base.org/base-chain/api-reference/debug-api/debug_traceTransaction
- https://docs.base.org/base-chain/api-reference/rpc-overview
- https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers

## Trace source

The worker calls:

```json
{
  "method": "debug_traceTransaction",
  "params": [
    "0x<transaction_hash>",
    {
      "tracer": "callTracer",
      "timeout": "20s",
      "tracerConfig": {
        "onlyTopCall": false,
        "withLog": true
      }
    }
  ]
}
```

`callTracer` provides a nested call tree instead of full opcode-level struct
logs. This is substantially smaller while preserving internal calls,
delegatecalls, calldata, return data, errors, revert reasons, gas use, value,
and emitted log counts.

The Base public endpoint may reject or rate-limit Debug API calls. Configure a
provider with Base archive history and `debug_traceTransaction`/`callTracer`
support before enabling the worker.

## Candidate selection and retry

Canonical transactions that contain a known DEX Swap are selected from local
ClickHouse facts. Completed traces are excluded. The latest sync state also
excludes dead-lettered transactions and retries transient failures only after
`next_retry_at`.

Retries use bounded exponential backoff:

```text
pending -> retry -> retry -> completed
                         -> dead_letter
```

The worker is sequential and rate-limited by default because trace replay is
expensive. Both the per-request interval and the batch poll interval are always
enforced, including after a complete batch of provider errors. Historical
replay begins at `TRACE_START_BLOCK`; ordering is deterministic by block
number, transaction index, and transaction hash.

## Call classification

Every call frame receives a deterministic trace address such as:

```text
0x<tx>:root
0x<tx>:0
0x<tx>:0.1
```

The analyzer records:

- `CALL`, `STATICCALL`, `DELEGATECALL`, and creation frame types;
- parent/child trace addresses and depth;
- four-byte function selector and known Router/Multicall function name;
- success, error, and revert reason;
- calls into pools observed in the transaction's canonical Swap logs;
- Router frames whose subtree reaches one of those pools;
- Multicall and Universal Router `execute` selectors;
- transaction summary counts and distinct Router addresses.

Router classification is evidence-based: a non-pool call is marked as a Router
only when its descendant call tree reaches a pool involved in that transaction.
Unknown selectors are retained without guessing a function name.

## ClickHouse tables

```text
transaction_call_traces
transaction_trace_summaries
transaction_trace_sync_state
```

The raw `callTracer` result is retained on the transaction summary for future
parser upgrades. Derived rows use `trace_version = call-v1`, allowing
deterministic reprocessing when classification rules change.

The Query API exposes the derived summary and call tree without returning the
potentially large raw trace:

```http
GET /api/v1/transactions/0x<64-hex-characters>/trace
```

Example queries:

```sql
SELECT
    transaction_hash,
    root_function,
    router_addresses,
    multicall_selectors,
    frame_count,
    max_depth
FROM transaction_trace_summaries FINAL
ORDER BY block_number DESC
LIMIT 20;
```

```sql
SELECT
    trace_address,
    call_type,
    from_address,
    to_address,
    function_name,
    is_router_call,
    is_pool_call,
    error
FROM transaction_call_traces FINAL
WHERE transaction_hash = '0x...'
ORDER BY trace_address;
```

## Configuration

```dotenv
ARCHIVE_RPC_URL=https://your-base-archive-debug-provider
TRACE_START_BLOCK=0
TRACE_BATCH_SIZE=5
TRACE_POLL_INTERVAL=10s
TRACE_REQUEST_TIMEOUT=30s
TRACE_TRACER_TIMEOUT=20s
TRACE_MIN_REQUEST_INTERVAL=1s
TRACE_MAX_ATTEMPTS=5
TRACE_RETRY_BASE=30s
TRACE_RETRY_MAX=30m
```

The service belongs to the optional `archive` Compose profile:

```shell
docker compose run --rm migrate
docker compose --profile archive up -d trace-worker
docker compose logs -f trace-worker
```

Do not enable the profile until `ARCHIVE_RPC_URL` points to a provider that
supports historical state and the Debug API.

## Next trace-based layers

The persisted call graph is the fact layer for:

1. replacing transaction-level wallet attribution with swap-frame attribution;
2. native/ERC-20 funding-source graph construction;
3. deployer, fee receiver, and developer-related address clustering;
4. deterministic historical strategy backtests.
