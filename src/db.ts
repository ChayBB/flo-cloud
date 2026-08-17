// Cloud-side storage — PostgreSQL via Bun's native SQL client (Bun.SQL).
// Matches the restaurant-OS design (Cloud = Elysia + PostgreSQL). All ops are
// async. Connect with connect(url); call initSchema once on boot.
import { randomUUID } from 'node:crypto';

export type Sql = InstanceType<typeof Bun.SQL>;

export interface Store {
  store_id: string;
  pos_hash: string;
  api_key: string;
  business: unknown;
  created_at: string;
}

export function connect(url = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/flo_cloud'): Sql {
  return new Bun.SQL(url);
}

export async function initSchema(sql: Sql): Promise<void> {
  await sql`
    CREATE TABLE IF NOT EXISTS stores (
      store_id   TEXT PRIMARY KEY,
      pos_hash   TEXT UNIQUE NOT NULL,
      api_key    TEXT NOT NULL,
      business   JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS edge_sync_events (
      id              UUID PRIMARY KEY,
      store_id        TEXT NOT NULL REFERENCES stores(store_id),
      idempotency_key TEXT NOT NULL,
      event_seq       BIGINT,
      event_type      TEXT NOT NULL,
      entity_type     TEXT NOT NULL,
      entity_id       TEXT NOT NULL,
      payload         JSONB NOT NULL,
      received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (store_id, idempotency_key)
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS edge_cursors (
      store_id TEXT PRIMARY KEY REFERENCES stores(store_id),
      last_seq BIGINT NOT NULL DEFAULT 0
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS edge_commands (
      id           TEXT PRIMARY KEY,
      store_id     TEXT NOT NULL REFERENCES stores(store_id),
      cmd          TEXT NOT NULL,
      payload      JSONB,
      status       TEXT NOT NULL DEFAULT 'QUEUED',
      result       JSONB,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      delivered_at TIMESTAMPTZ,
      acked_at     TIMESTAMPTZ
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS seen_nonces (
      nonce   TEXT PRIMARY KEY,
      seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`;
}

/** Test helper: wipe all rows (keeps schema). */
export async function resetSchema(sql: Sql): Promise<void> {
  await sql`TRUNCATE edge_sync_events, edge_cursors, edge_commands, seen_nonces, stores RESTART IDENTITY CASCADE`;
}

export async function registerStore(sql: Sql, posHash: string, business: unknown): Promise<Store> {
  const storeId = `st_${randomUUID()}`;
  const apiKey = `sk_${randomUUID().replace(/-/g, '')}`;
  // Insert-or-recognise: a pos_hash already seen keeps its identity.
  const rows = await sql`
    INSERT INTO stores (store_id, pos_hash, api_key, business)
    VALUES (${storeId}, ${posHash}, ${apiKey}, ${JSON.stringify(business ?? {})}::jsonb)
    ON CONFLICT (pos_hash) DO NOTHING
    RETURNING store_id, pos_hash, api_key, business, created_at`;
  if (rows.length) return rows[0] as Store;
  const existing = await sql`SELECT store_id, pos_hash, api_key, business, created_at FROM stores WHERE pos_hash = ${posHash}`;
  return existing[0] as Store;
}

export async function storeByHash(sql: Sql, posHash: string): Promise<Store | null> {
  const rows = await sql`SELECT store_id, pos_hash, api_key, business, created_at FROM stores WHERE pos_hash = ${posHash}`;
  return (rows[0] as Store) ?? null;
}

/** Record a nonce; false if already seen (replay). */
export async function claimNonce(sql: Sql, nonce: string): Promise<boolean> {
  const rows = await sql`INSERT INTO seen_nonces (nonce) VALUES (${nonce}) ON CONFLICT (nonce) DO NOTHING RETURNING nonce`;
  return rows.length > 0;
}

export async function getCursor(sql: Sql, storeId: string): Promise<number> {
  const rows = await sql`SELECT last_seq FROM edge_cursors WHERE store_id = ${storeId}`;
  return rows.length ? Number(rows[0].last_seq) : 0;
}

export interface ApplyResult { applied: number; deduped: number; ignored: number; appliedSeq: number }

/**
 * Apply a batch for one store, in one transaction. Idempotent on
 * (store_id, idempotency_key); ordered by event_seq (events ≤ cursor ignored).
 * Legacy events without a seq get a synthetic key and never move the cursor.
 */
export async function applyEvents(sql: Sql, storeId: string, events: any[]): Promise<ApplyResult> {
  let applied = 0, deduped = 0, ignored = 0;
  const seqOf = (e: any) => (typeof e?.payload?.event_seq === 'number' ? e.payload.event_seq : null);
  const sorted = [...events].sort((a, b) => (seqOf(a) ?? Infinity) - (seqOf(b) ?? Infinity));

  const appliedSeq = await sql.begin(async (tx: Sql) => {
    const cur = await tx`SELECT last_seq FROM edge_cursors WHERE store_id = ${storeId} FOR UPDATE`;
    let cursor = cur.length ? Number(cur[0].last_seq) : 0;
    for (const e of sorted) {
      const s = seqOf(e);
      const key = e?.payload?.idempotency_key ?? `${e.entity_type}:${e.entity_id}:${e.type}:${e.id}`;
      if (s !== null && s <= cursor) { ignored++; continue; }
      const ins = await tx`
        INSERT INTO edge_sync_events (id, store_id, idempotency_key, event_seq, event_type, entity_type, entity_id, payload)
        VALUES (${randomUUID()}, ${storeId}, ${key}, ${s}, ${e.type}, ${e.entity_type}, ${e.entity_id}, ${JSON.stringify(e.payload ?? {})}::jsonb)
        ON CONFLICT (store_id, idempotency_key) DO NOTHING
        RETURNING id`;
      if (ins.length) { applied++; if (s !== null && s > cursor) cursor = s; }
      else deduped++;
    }
    await tx`
      INSERT INTO edge_cursors (store_id, last_seq) VALUES (${storeId}, ${cursor})
      ON CONFLICT (store_id) DO UPDATE SET last_seq = EXCLUDED.last_seq`;
    return cursor;
  });

  return { applied, deduped, ignored, appliedSeq: appliedSeq as number };
}

export async function queueCommand(sql: Sql, storeId: string, cmd: string, payload: unknown): Promise<string> {
  const id = `cmd_${randomUUID()}`;
  await sql`INSERT INTO edge_commands (id, store_id, cmd, payload, status) VALUES (${id}, ${storeId}, ${cmd}, ${JSON.stringify(payload ?? {})}::jsonb, 'QUEUED')`;
  return id;
}

export async function pollCommands(sql: Sql, storeId: string, limit: number): Promise<any[]> {
  // Claim up to `limit` queued/delivered commands and mark them delivered.
  const rows = await sql`
    UPDATE edge_commands SET status = 'DELIVERED', delivered_at = now()
    WHERE id IN (
      SELECT id FROM edge_commands
      WHERE store_id = ${storeId} AND status IN ('QUEUED','DELIVERED')
      ORDER BY created_at LIMIT ${limit}
    )
    RETURNING id, cmd, payload`;
  // jsonb comes back as a string from the driver; hand callers a parsed object.
  for (const r of rows as any[]) {
    if (typeof r.payload === 'string') { try { r.payload = JSON.parse(r.payload); } catch { /* leave as-is */ } }
  }
  return rows as any[];
}

/** Ack a command with its result. Idempotent; false if the id is unknown. */
export async function ackCommand(sql: Sql, storeId: string, id: string, result: unknown): Promise<boolean> {
  const upd = await sql`
    UPDATE edge_commands SET status = 'ACKED', result = ${JSON.stringify(result ?? {})}::jsonb, acked_at = now()
    WHERE id = ${id} AND store_id = ${storeId} RETURNING id`;
  if (upd.length) return true;
  const exists = await sql`SELECT 1 FROM edge_commands WHERE id = ${id} AND store_id = ${storeId}`;
  return exists.length > 0;
}
