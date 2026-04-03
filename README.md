# Distributed Inventory and Orders with 2PC

This project implements a distributed transaction workflow using the Two-Phase Commit (2PC) protocol across microservices.

## Services

- `coordinator`: drives the 2PC protocol, writes WAL, exposes transaction APIs, broadcasts WebSocket updates.
- `inventory`: participant service that validates and applies stock updates.
- `orders`: participant service that creates and confirms orders.
- `inventory-db` (PostgreSQL): products and stock data.
- `orders-db` (PostgreSQL): orders and order items data.
- `frontend`: real-time transaction monitor UI.

## How to Run

```bash
docker compose up --build -d
```

Equivalent command (older Docker Compose CLI):

```bash
docker-compose up --build -d
```

Check service status:

```bash
docker compose ps
```

Stop the stack:

```bash
docker compose down
```

Frontend:
- `http://localhost:8081`

Coordinator API:
- `http://localhost:3000`

## Environment Variables

Use the root `.env.example` as the single reference for all environment variables used by this application.

- Coordinator: `CORS_ORIGINS`, `WAL_FILE`, `PARTICIPANT_REQUEST_TIMEOUT_MS`
- Participants: `COORDINATOR_URL`, `TXN_STATE_FILE` (service-specific value)
- Frontend: `REACT_APP_API_URL`, `REACT_APP_WS_URL`

Service-local `.env.example` files are also included under each service folder for convenience.

## Core Endpoints

### Coordinator

- `POST /api/transactions`
- `GET /api/transactions/:id`
- `POST /chaos/kill`

### Transaction Payload Rules

- `items` must be a non-empty array.
- Max 100 entries per transaction.
- `sku` is uppercased and limited to `A-Z`, `0-9`, `_`, `-`.
- `quantity` must be a positive integer.
- Seeded inventory currently contains `SKU001` through `SKU050`.

### Inventory Participant

- `POST /prepare`
- `POST /commit`
- `POST /rollback`

### Orders Participant

- `POST /prepare`
- `POST /commit`
- `POST /rollback`

## WAL Format

The coordinator appends JSON lines to `./data/coordinator.log`:

```json
{"timestamp":"...","transaction_id":"...","state":"BEGIN","payload":{"items":[...]}}
{"timestamp":"...","transaction_id":"...","state":"GLOBAL_COMMIT"}
{"timestamp":"...","transaction_id":"...","state":"END"}
```

## Recovery Behavior

### Coordinator Recovery

On startup, coordinator reads WAL and completes unfinished transactions:

- Last state `BEGIN` -> writes `GLOBAL_ABORT` and sends rollback.
- Last state `GLOBAL_COMMIT` -> resends commit.
- Last state `GLOBAL_ABORT` -> resends rollback.

### Participant Reconciliation

Participants persist local transaction state and on startup reconcile `PREPARED` transactions with coordinator status endpoint.

## Production Notes

- Startup ordering is health-gated in `docker-compose.yml` so app services wait for healthy dependencies.
- PostgreSQL health checks target the configured DB names (`inventory_db`, `orders_db`).
- Coordinator applies request validation and participant call timeouts to avoid hanging requests.

## Troubleshooting

- If every transaction aborts unexpectedly, check running containers: `docker compose ps`.
- If a participant logs `ENOTFOUND inventory-db` or `ENOTFOUND orders-db`, restart full stack: `docker compose up -d --build`.
- If inventory rejects an SKU, verify it exists in `docker/inventory-init/init.sql`.

## 2PC Blocking Problem

2PC can block when coordinator fails at a critical time. Example:

1. All participants vote `VOTE_COMMIT`.
2. Coordinator crashes before reliably telling participants final decision.
3. Participants may stay in uncertain state (`PREPARED`) and can hold resources.

This is a classic availability weakness of 2PC. It guarantees strong atomicity but may reduce liveness under failures.

## 2PC vs Saga

### 2PC

- Strong consistency and atomic commit/abort.
- Simpler business rollback semantics.
- Can block during coordinator/participant failures.
- Better for strict integrity domains (finance, critical inventory, ledger-like operations).

### Saga

- Eventual consistency with compensating actions.
- More available and resilient under partial failures.
- Requires careful compensation design and side-effect handling.
- Better for high-scale business workflows where short-lived inconsistencies are acceptable.

## Chaos Testing

`POST /chaos/kill` can stop selected participant containers (`inventory-service` / `orders-service`, and also `inventory` / `orders` for backward compatibility) via Docker socket to simulate failures and observe transaction behavior from the UI.
