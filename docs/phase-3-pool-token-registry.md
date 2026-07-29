# Phase 3: pool and token registry

The event parser enriches each newly observed pool with immutable on-chain
metadata before writing the normalized swap.

## Resolution flow

```text
pool address
  -> token0()
  -> token1()
  -> factory()
  -> verified factory registry
  -> token decimals()/symbol()
  -> PostgreSQL cache
  -> ClickHouse dex_pool_swaps
```

Pool and token calls use small JSON-RPC batches and a global request limiter.
This keeps the public Base RPC fallback usable while allowing a production RPC
endpoint to be configured through `BASE_HTTP_URL`.

Each block has a bounded enrichment budget, configured with
`REGISTRY_ENRICHMENT_TIMEOUT` (default `1s`). Cached metadata is applied
immediately. If the budget expires, the swap is still persisted as
`unresolved` and its pool is retried on a later observation. This prevents
public RPC throttling from blocking the Kafka fact-data path.

## PostgreSQL tables

- `dex_factories`: audited factory-to-protocol mappings;
- `dex_pools`: immutable pool token and factory metadata;
- `token_metadata`: token symbol and decimals, including known/unknown flags.

Partial token metadata is not permanently cached in memory. Missing values are
retried on later observations.

## Seeded Base factories

| Protocol | Version | Factory |
|---|---|---|
| Aerodrome | Classic | `0x420dd381b31aef6683db6b902084cb0ffec40da` |
| Aerodrome | Slipstream | `0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a` |
| Uniswap | V3 | `0x33128a8fc17869897dce68ed026d694621f6fdfd` |

Unknown factories remain `protocol = 'unknown'`. They can be reviewed and
added to `dex_factories` without changing the parser.

## Swap columns

`base.dex_pool_swaps` now includes:

- `factory_address`;
- `protocol` and `protocol_version`;
- `token0_address` and `token1_address`;
- token symbols and decimals;
- `metadata_status`: `resolved`, `partial`, or `unresolved`.

The raw delta convention is unchanged: positive values enter the pool and
negative values leave it.

## Operational queries

```sql
SELECT protocol, protocol_version, count()
FROM base.dex_pool_swaps FINAL
GROUP BY protocol, protocol_version;
```

```sql
SELECT metadata_status, count()
FROM base.dex_pool_swaps FINAL
GROUP BY metadata_status;
```

```sql
SELECT factory_address, count()
FROM base.dex_pool_swaps FINAL
WHERE protocol = 'unknown'
GROUP BY factory_address
ORDER BY count() DESC;
```
