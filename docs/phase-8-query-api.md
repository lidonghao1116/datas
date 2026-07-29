# Phase 8: Query API and WebSocket gateway

`query-api` exposes the processed Base analytics data without making clients
depend directly on PostgreSQL or ClickHouse.

## Data flow

```text
PostgreSQL alert_outbox
  -> REST alert queries
  -> incremental cursor poller
  -> WebSocket alert broadcast

ClickHouse current views and snapshots
  -> large-trade queries
  -> latest token market and risk queries
```

The WebSocket poller starts at service startup and only broadcasts alerts
created after that point. Clients use the REST endpoint to load history before
subscribing to live updates.

## Endpoints

### Health

```http
GET /healthz
```

Returns `200` only when both PostgreSQL and ClickHouse are reachable.

### Alerts

```http
GET /api/v1/alerts?status=pending&severity=critical&type=large_buy&limit=50
```

All filters are optional. `limit` defaults to 50 and is capped at 200.

### Large trades

```http
GET /api/v1/trades/large?limit=50
```

Returns the latest USD-valued swaps marked as large trades.

### Token market and risk

```http
GET /api/v1/tokens/{address}/market
```

Returns the latest AVE market snapshot and, when available, the latest
contract-risk snapshot for a Base token.

### Live alerts

```text
ws://localhost:8080/ws/alerts
```

Messages use this envelope:

```json
{
  "type": "alert",
  "data": {
    "alert_key": "...",
    "alert_type": "large_buy",
    "severity": "critical"
  }
}
```

The server sends WebSocket ping frames and disconnects clients that stop
reading or whose bounded outbound queue fills.

## Configuration

```dotenv
API_LISTEN_ADDRESS=:8080
API_ALERT_POLL_INTERVAL=1s
API_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
```

Origins are enforced for browser CORS and WebSocket upgrades. A literal `*`
allows every origin and should only be used for local development.

## Run

```powershell
docker compose up -d --build query-api
Invoke-RestMethod http://localhost:8080/healthz
Invoke-RestMethod "http://localhost:8080/api/v1/alerts?limit=10"
```
