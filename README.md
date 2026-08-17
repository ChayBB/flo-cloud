# flo-cloud — edge-sync receiver (prototype)
n[![CI](https://github.com/ChayBB/flo-cloud/actions/workflows/ci.yml/badge.svg)](https://github.com/ChayBB/flo-cloud/actions/workflows/ci.yml)

Bun + Elysia + **PostgreSQL** prototype of the FloPOS **cloud** side of the
restaurant-OS edge-sync protocol. Implements the contract in the FloROS repo
`docs/openapi/flopos-cloud.yaml`, derived from the edge client.

Storage is PostgreSQL via Bun's native `Bun.SQL` (no ORM, no extra deps),
matching the design (`docs/restaurant-os/01-architecture.md`: Cloud = Elysia +
PostgreSQL).

## Setup

Needs a reachable PostgreSQL. Defaults assume local `postgres:postgres`.

```sh
bun install
# create databases (first run only)
bun -e 'const s=new Bun.SQL("postgres://postgres:postgres@localhost:5432/postgres"); \
  for (const n of ["flo_cloud","flo_cloud_test"]) { \
    if ((await s`SELECT 1 FROM pg_database WHERE datname=${n}`).length===0) await s.unsafe(`CREATE DATABASE ${n}`); } \
  await s.end();'

# run (schema auto-created on boot)
DATABASE_URL=postgres://postgres:postgres@localhost:5432/flo_cloud bun run src/server.ts

# test (against flo_cloud_test)
bun test
```

Env:
- `DATABASE_URL` — server storage (default `…/flo_cloud`)
- `TEST_DATABASE_URL` — test storage (default `…/flo_cloud_test`)
- `PORT` — HTTP port (default 8787)

## Endpoints
- `POST /api/pos/register` — onboarding (unauth), returns `store_id` + `api_key`
- `POST /api/pos/events` — event sink v2 (order + table_session); idempotent on
  `(store, idempotency_key)`, ordered by `event_seq` (cursor)
- `POST /api/pos/heartbeat`
- `GET  /api/pos/commands?limit=` — poll (HTTP fallback for the WSS relay)
- `POST /api/pos/commands/{id}/result` — ack + result (idempotent)
- `POST /internal/commands` — test helper to queue a command (not part of the edge contract)

## Schema (PostgreSQL)
`stores`, `edge_sync_events` (unique `(store_id, idempotency_key)`),
`edge_cursors` (per-store `last_seq`), `edge_commands`, `seen_nonces`.

Auth: HMAC signing identical to the edge (`X-Flo-Signature` = `sha256=HMAC(api_key,
METHOD\npath\ntimestamp\nnonce\nsha256(body))`) with body-hash, timestamp-skew,
and nonce-replay checks.
