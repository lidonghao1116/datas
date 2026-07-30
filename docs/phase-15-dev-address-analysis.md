# Phase 15: Dev and related-address analysis

This phase builds an explainable, one-hop Dev relationship graph from local
Base facts and optional enrichment data. It does not recursively expand wallet
neighbors and does not label an address as a team member without retaining the
underlying evidence.

## Primary deployer

The primary deployer is reconstructed from canonical chain facts:

```text
raw_receipts.contract_address = token
  -> raw_receipts.transaction_hash
  -> raw_transactions.from_address = primary_deployer
```

Only successfully deployed contracts that later appear as Token addresses in
canonical DEX Swap facts are analyzed. This avoids relying on replaceable API
metadata for the primary Dev identity.

## One-hop evidence

`dev-v1` currently records:

| Relation | Source | Base weight | Meaning |
| --- | --- | ---: | --- |
| `deployer` | canonical Receipt/transaction | 1.00 | transaction sender that deployed the Token |
| `gmgn_funder` | GMGN profile | 0.90 | externally reported first funding address |
| `native_funder` | canonical transaction | 0.75 | address that sent native value to the deployer |
| `trace_caller` | callTracer | 0.65 | successful internal caller of the deployer |
| `trace_callee` | callTracer | 0.50 | successful internal target called by the deployer |
| `erc20_sender` | ERC-20 Transfer | 0.45 | address that transferred tokens to the deployer |
| `erc20_receiver` | ERC-20 Transfer | 0.35 | address that received tokens from the deployer |

Repeated evidence adds up to `0.15`, and confidence is capped at `0.99`.
Relationships with confidence of at least `0.70` count as strong.

Native and ERC-20 evidence is bounded to the deployment period through 30 days
after deployment. Trace evidence becomes available as the optional Archive
Trace worker fills `transaction_call_traces`.

The analysis never recursively traverses a related address. This prevents
exchange deposit addresses, routers, pools, and ordinary counterparties from
causing uncontrolled graph expansion.

## Address kind

The analyzer marks an address as `contract` only when local canonical evidence
confirms it as one of:

- a contract creation Receipt address;
- a known DEX pool;
- a Token observed in a DEX pool.

The primary deployer is `wallet`. All other unconfirmed addresses remain
`unknown`; absence from the local contract set is not treated as proof that an
address is an EOA.

## Dev risk score

The score is a deterministic heuristic, not an accusation or a security audit:

| Component | Points |
| --- | ---: |
| any deployed Token marked honeypot | 45 |
| any deployed Token with blacklist method | 20 |
| any deployed Token with mint method | 10 |
| any deployed Token marked proxy | 5 |
| additional traded Tokens from the same deployer | 5 each, maximum 15 |
| strong related addresses | 1 each, maximum 5 |

The total is capped at 100:

```text
0-19   low
20-44  medium
45-69  high
70-100 critical
```

Confidence is stored separately and is based on distinct evidence sources plus
the availability of AVE risk snapshots.

## ClickHouse tables

```text
token_dev_relationships
token_dev_profiles
```

Both tables use `analysis_version = dev-v1` and `ReplacingMergeTree`, allowing
the same Token to be recalculated as GMGN, Trace, Transfer, or risk evidence
changes.

## Query API

```http
GET /api/v1/tokens/0x<token_address>/dev
```

The response contains the Token-level profile, risk components, confidence,
and every one-hop relationship with evidence count, direction, source,
timestamps, and sample transaction hashes.

## Configuration

```dotenv
DEV_ANALYSIS_BATCH_SIZE=10
DEV_ANALYSIS_EVIDENCE_LIMIT=50
DEV_ANALYSIS_POLL_INTERVAL=30s
DEV_ANALYSIS_REFRESH_INTERVAL=6h
```

Run:

```shell
docker compose run --rm migrate
docker compose up -d dev-analysis-worker query-api
docker compose logs -f dev-analysis-worker
```

## Current boundary

- The graph is intentionally one hop.
- `unknown` addresses are not assumed to be wallets.
- GMGN remains a replaceable enhancement source.
- AVE flags contribute to a transparent heuristic; they do not replace
  canonical deployment, transaction, Transfer, and Trace facts.
- Ownership changes, fee receiver extraction from Token-specific storage, and
  proxy implementation ownership require additional contract-state analysis.
