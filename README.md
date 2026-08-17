# flo-cloud — edge-sync receiver (prototype)

Bun + Elysia + SQLite prototype of the FloPOS **cloud** side of the restaurant-OS
edge-sync protocol. Implements the contract in the FloROS repo
`docs/openapi/flopos-cloud.yaml`, derived from the edge client.

Production would use PostgreSQL (see FloROS `docs/restaurant-os/01-architecture.md`);
this uses `bun:sqlite` so it runs and tests with zero infra.

## Run
```sh
bun install
bun run src/server.ts      # http://localhost:8787
bun test                   # receiver test suite
```

## Endpoints
- `POST /api/pos/register` — onboarding (unauth), returns `store_id` + `api_key`
- `POST /api/pos/events` — event sink v2 (order + table_session); idempotent on
  `(store, idempotency_key)`, ordered by `event_seq` (cursor)
- `POST /api/pos/heartbeat`
- `GET  /api/pos/commands?limit=` — poll (HTTP fallback for the WSS relay)
- `POST /api/pos/commands/{id}/result` — ack + result (idempotent)
- `POST /internal/commands` — test helper to queue a command (not part of the edge contract)

Auth: HMAC signing identical to the edge (`X-Flo-Signature` = `sha256=HMAC(api_key,
METHOD\npath\ntimestamp\nnonce\nsha256(body))`), with body-hash, timestamp-skew,
and nonce-replay checks.
