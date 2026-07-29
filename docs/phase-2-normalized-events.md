# Phase 2: normalized on-chain events

The `event-parser` service consumes `base.block.raw.v1` with its own Kafka
consumer group and writes normalized events to ClickHouse.

## Supported events

### ERC-20 Transfer

Signature:

```text
Transfer(address indexed from, address indexed to, uint256 value)
```

Only the canonical three-topic ERC-20 layout is accepted. ERC-721 Transfer
events have an indexed token ID and four topics, so they are intentionally
ignored.

Destination table:

```text
base.erc20_transfers
```

### Uniswap V2-compatible Swap

Signature:

```text
Swap(
  address indexed sender,
  uint256 amount0In,
  uint256 amount1In,
  uint256 amount0Out,
  uint256 amount1Out,
  address indexed to
)
```

This includes pools using the same event layout, such as Aerodrome Classic.
Without a verified pool registry, the event is labeled
`uniswap_v2_compatible` instead of being assigned to a specific protocol.

### Uniswap V3-compatible Swap

Signature:

```text
Swap(
  address indexed sender,
  address indexed recipient,
  int256 amount0,
  int256 amount1,
  uint160 sqrtPriceX96,
  uint128 liquidity,
  int24 tick
)
```

This includes compatible concentrated-liquidity pools. Events are labeled
`uniswap_v3_compatible` until the pool registry resolves the exact protocol.

Destination table:

```text
base.dex_pool_swaps
```

## Amount semantics

`amount0_delta_raw` and `amount1_delta_raw` represent the pool balance change:

- positive: token entered the pool;
- negative: token left the pool.

Values are unscaled integers. Token addresses, decimals, symbols and decimal
amounts will be attached by the pool/token registry stage.

## Run

Apply migrations and start the parser:

```powershell
docker compose run --rm migrate
docker compose up -d --build event-parser
```

Verify:

```powershell
docker exec base-analytics-clickhouse-1 clickhouse-client --database base `
  --query "SELECT protocol_family, count() FROM dex_pool_swaps GROUP BY protocol_family"
```

The consumer group defaults to `base-event-parser-v1`, so it can replay the
raw block topic independently from `block-writer`.

