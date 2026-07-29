# Phase 7: alert outbox dispatcher

The `alert-dispatcher` delivers PostgreSQL Outbox rows to a generic HTTP
webhook with retry and crash-recovery guarantees.

## Claim and lease

Workers atomically claim rows with:

```sql
FOR UPDATE SKIP LOCKED
```

Claimed rows enter `processing`, increment `attempts`, and receive `locked_at`
and `locked_by`. If a process exits before ACK/NACK, another worker can reclaim
the row after `ALERT_DELIVERY_LEASE`.

## Webhook contract

Each delivery is a JSON object containing the alert fields, `attempt`, and
`created_at`.

Headers:

- `Content-Type: application/json`;
- `Idempotency-Key: <alert_key>`;
- `X-Alert-Key: <alert_key>`;
- `X-Alert-Signature-SHA256: <hex HMAC>` when a secret is configured.

The signature is `HMAC-SHA256(raw_request_body, ALERT_WEBHOOK_SECRET)`.
Any 2xx response ACKs the row as `delivered`.

## Retry policy

Non-2xx responses, timeouts, and network failures move the row to `failed`.
Retry delay grows exponentially from `ALERT_DELIVERY_RETRY_BASE` up to
`ALERT_DELIVERY_RETRY_MAX`. After `ALERT_DELIVERY_MAX_ATTEMPTS`, the row enters
`dead_letter` and requires operator review.

## Configuration

```dotenv
ALERT_WEBHOOK_URL=
ALERT_WEBHOOK_SECRET=
ALERT_DELIVERY_BATCH_SIZE=20
ALERT_DELIVERY_LEASE=30s
ALERT_DELIVERY_POLL_INTERVAL=2s
ALERT_DELIVERY_TIMEOUT=10s
ALERT_DELIVERY_MAX_ATTEMPTS=8
ALERT_DELIVERY_RETRY_BASE=5s
ALERT_DELIVERY_RETRY_MAX=15m
```

When `ALERT_WEBHOOK_URL` is empty, the service remains alive in disabled mode
and does not claim pending rows.

## Operations

```shell
docker compose up -d alert-dispatcher
docker compose logs -f alert-dispatcher
```

Delivery state:

```sql
SELECT status, count(*), sum(attempts)
FROM alert_outbox
GROUP BY status;
```

Dead letters:

```sql
SELECT alert_key, attempts, last_error, updated_at
FROM alert_outbox
WHERE status = 'dead_letter'
ORDER BY updated_at DESC;
```

An operator can retry a dead letter after correcting the downstream issue:

```sql
UPDATE alert_outbox
SET status = 'failed',
    next_attempt_at = now(),
    locked_at = NULL,
    locked_by = '',
    updated_at = now()
WHERE alert_key = $1 AND status = 'dead_letter';
```
