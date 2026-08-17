// Cloud-side storage for the edge-sync receiver. bun:sqlite for the prototype
// (production would be PostgreSQL per docs/restaurant-os/01-architecture.md).
// Tables model the server side deferred in the restaurant-OS design:
// edge_sync_events, edge_cursors, edge_commands.
import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';

export interface Store {
  store_id: string;
  pos_hash: string;
  api_key: string;
  business: string; // JSON
  created_at: string;
}

export function openDb(path = ':memory:'): Database {
  const db = new Database(path);
  db.exec('PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;');
  db.exec(`
    CREATE TABLE IF NOT EXISTS stores (
      store_id   TEXT PRIMARY KEY,
      pos_hash   TEXT UNIQUE NOT NULL,
      api_key    TEXT NOT NULL,
      business   TEXT,
      created_at TEXT NOT NULL
    );
    -- Accepted events, unique per store on the edge's idempotency key.
    CREATE TABLE IF NOT EXISTS edge_sync_events (
      id              TEXT PRIMARY KEY,
      store_id        TEXT NOT NULL,
      idempotency_key TEXT NOT NULL,
      event_seq       INTEGER,
      event_type      TEXT NOT NULL,
      entity_type     TEXT NOT NULL,
      entity_id       TEXT NOT NULL,
      payload         TEXT NOT NULL,
      received_at     TEXT NOT NULL,
      UNIQUE (store_id, idempotency_key)
    );
    -- Last applied monotonic sequence per store (the cursor).
    CREATE TABLE IF NOT EXISTS edge_cursors (
      store_id TEXT PRIMARY KEY,
      last_seq INTEGER NOT NULL DEFAULT 0
    );
    -- Cloud -> edge command queue.
    CREATE TABLE IF NOT EXISTS edge_commands (
      id           TEXT PRIMARY KEY,
      store_id     TEXT NOT NULL,
      cmd          TEXT NOT NULL,
      payload      TEXT,
      status       TEXT NOT NULL DEFAULT 'QUEUED',
      result       TEXT,
      created_at   TEXT NOT NULL,
      delivered_at TEXT,
      acked_at     TEXT
    );
    -- Seen nonces for replay protection (bounded by timestamp skew in practice).
    CREATE TABLE IF NOT EXISTS seen_nonces (
      nonce    TEXT PRIMARY KEY,
      seen_at  TEXT NOT NULL
    );
  `);
  return db;
}

const now = () => new Date().toISOString();

export function registerStore(db: Database, posHash: string, business: unknown): Store {
  const existing = db.query('SELECT * FROM stores WHERE pos_hash = ?').get(posHash) as Store | null;
  if (existing) return existing;
  const store: Store = {
    store_id: `st_${randomUUID()}`,
    pos_hash: posHash,
    api_key: `sk_${randomUUID().replace(/-/g, '')}`,
    business: JSON.stringify(business ?? {}),
    created_at: now(),
  };
  db.query(
    'INSERT INTO stores (store_id, pos_hash, api_key, business, created_at) VALUES (?, ?, ?, ?, ?)',
  ).run(store.store_id, store.pos_hash, store.api_key, store.business, store.created_at);
  return store;
}

export function storeByHash(db: Database, posHash: string): Store | null {
  return db.query('SELECT * FROM stores WHERE pos_hash = ?').get(posHash) as Store | null;
}

/** Record a nonce; returns false if already seen (replay). */
export function claimNonce(db: Database, nonce: string): boolean {
  try {
    db.query('INSERT INTO seen_nonces (nonce, seen_at) VALUES (?, ?)').run(nonce, now());
    return true;
  } catch {
    return false;
  }
}

export function getCursor(db: Database, storeId: string): number {
  const row = db.query('SELECT last_seq FROM edge_cursors WHERE store_id = ?').get(storeId) as
    | { last_seq: number }
    | null;
  return row?.last_seq ?? 0;
}

export interface ApplyResult { applied: number; deduped: number; ignored: number; appliedSeq: number }

/**
 * Apply a batch of events for one store. Idempotent on (store_id,
 * idempotency_key); ordered by event_seq (events at or below the cursor are
 * ignored). Events without a seq/key (legacy order snapshots) are stored by a
 * synthetic key and never advance the cursor.
 */
export function applyEvents(db: Database, storeId: string, events: any[]): ApplyResult {
  let applied = 0, deduped = 0, ignored = 0;
  const insert = db.query(`
    INSERT INTO edge_sync_events
      (id, store_id, idempotency_key, event_seq, event_type, entity_type, entity_id, payload, received_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const txn = db.transaction((batch: any[]) => {
    let cursor = getCursor(db, storeId);
    // Sequenced events applied in order; unsequenced (legacy) appended as-is.
    const seq = (e: any) => (typeof e?.payload?.event_seq === 'number' ? e.payload.event_seq : null);
    const sorted = [...batch].sort((a, b) => (seq(a) ?? Infinity) - (seq(b) ?? Infinity));
    for (const e of sorted) {
      const s = seq(e);
      const key = e?.payload?.idempotency_key ?? `${e.entity_type}:${e.entity_id}:${e.type}:${e.id}`;
      if (s !== null && s <= cursor) { ignored++; continue; }
      try {
        insert.run(randomUUID(), storeId, key, s, e.type, e.entity_type, e.entity_id, JSON.stringify(e.payload ?? {}), now());
        applied++;
        if (s !== null && s > cursor) cursor = s;
      } catch {
        deduped++; // UNIQUE(store_id, idempotency_key) violation = already applied
      }
    }
    db.query(
      'INSERT INTO edge_cursors (store_id, last_seq) VALUES (?, ?) ON CONFLICT(store_id) DO UPDATE SET last_seq = excluded.last_seq',
    ).run(storeId, cursor);
    return cursor;
  });
  const appliedSeq = txn(events);
  return { applied, deduped, ignored, appliedSeq };
}

export function queueCommand(db: Database, storeId: string, cmd: string, payload: unknown): string {
  const id = `cmd_${randomUUID()}`;
  db.query('INSERT INTO edge_commands (id, store_id, cmd, payload, status, created_at) VALUES (?, ?, ?, ?, ?, ?)')
    .run(id, storeId, cmd, JSON.stringify(payload ?? {}), 'QUEUED', now());
  return id;
}

export function pollCommands(db: Database, storeId: string, limit: number): any[] {
  const rows = db.query(
    "SELECT id, cmd, payload FROM edge_commands WHERE store_id = ? AND status IN ('QUEUED','DELIVERED') ORDER BY created_at LIMIT ?",
  ).all(storeId, limit) as any[];
  const mark = db.query("UPDATE edge_commands SET status = 'DELIVERED', delivered_at = ? WHERE id = ?");
  for (const r of rows) { mark.run(now(), r.id); r.payload = JSON.parse(r.payload ?? '{}'); }
  return rows;
}

/** Ack a command with its result. Idempotent. Returns false if the id is unknown. */
export function ackCommand(db: Database, storeId: string, id: string, result: unknown): boolean {
  const res = db.query(
    "UPDATE edge_commands SET status = 'ACKED', result = ?, acked_at = ? WHERE id = ? AND store_id = ?",
  ).run(JSON.stringify(result ?? {}), now(), id, storeId);
  if (res.changes > 0) return true;
  // Already acked earlier? still true (idempotent); truly unknown -> false.
  return !!db.query('SELECT 1 FROM edge_commands WHERE id = ? AND store_id = ?').get(id, storeId);
}
