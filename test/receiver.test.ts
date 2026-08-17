import { describe, it, expect, beforeEach } from 'bun:test';
import { createApp, getCursor } from '../src/server';
import { openDb } from '../src/db';
import { buildSignedHeaders } from '../src/sign';

const BASE = 'http://localhost';
const POS_HASH = 'poshash-abc';

let db: ReturnType<typeof openDb>;
let app: ReturnType<typeof createApp>;

async function register() {
  const res = await app.handle(new Request(`${BASE}/api/pos/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Flo-POS-Hash': POS_HASH },
    body: JSON.stringify({ business: { name: 'Test Cafe', country: 'TH' } }),
  }));
  return res.json() as Promise<{ store_id: string; api_key: string }>;
}

async function signedRequest(apiKey: string, method: string, path: string, bodyObj?: unknown) {
  const body = bodyObj === undefined ? '' : JSON.stringify(bodyObj);
  const headers = buildSignedHeaders(apiKey, POS_HASH, method, path, body);
  return app.handle(new Request(`${BASE}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', ...headers },
    body: method === 'GET' ? undefined : body,
  }));
}

function sessionEvent(seq: number, type: 'session.opened' | 'session.closed', sessionId: string) {
  const idempotency_key = `table_session:${sessionId}:${type}:${seq}`;
  return {
    id: `row-${seq}`,
    type,
    entity_type: 'table_session',
    entity_id: sessionId,
    payload: { event_seq: seq, idempotency_key, session_no: `S-${seq}`, table_id: 'T1' },
  };
}

beforeEach(() => {
  db = openDb(':memory:');
  app = createApp(db);
});

describe('onboarding', () => {
  it('register returns store_id + api_key; idempotent per pos_hash', async () => {
    const a = await register();
    expect(a.store_id).toStartWith('st_');
    expect(a.api_key).toStartWith('sk_');
    const b = await register();
    expect(b.store_id).toBe(a.store_id); // same identity, not duplicated
  });

  it('register without business is 400', async () => {
    const res = await app.handle(new Request(`${BASE}/api/pos/register`, {
      method: 'POST', headers: { 'X-Flo-POS-Hash': POS_HASH }, body: '{}',
    }));
    expect(res.status).toBe(400);
  });
});

describe('events v2 — table_session, idempotency, cursor', () => {
  it('accepts a signed batch and advances the cursor', async () => {
    const { store_id, api_key } = await register();
    const res = await signedRequest(api_key, 'POST', '/api/pos/events', {
      pos_hash: POS_HASH, sent_at: new Date().toISOString(),
      events: [sessionEvent(1, 'session.opened', 'sess-1'), sessionEvent(2, 'session.closed', 'sess-1')],
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.applied).toBe(2);
    expect(body.applied_seq).toBe(2);
    expect(getCursor(db, store_id)).toBe(2);
  });

  it('re-delivering the same batch is idempotent (dedupe, cursor unchanged)', async () => {
    const { store_id, api_key } = await register();
    const batch = { pos_hash: POS_HASH, sent_at: '', events: [sessionEvent(1, 'session.opened', 'sess-1')] };
    await signedRequest(api_key, 'POST', '/api/pos/events', batch);
    const res = await signedRequest(api_key, 'POST', '/api/pos/events', batch);
    const body = await res.json();
    // Re-applied nothing — whether short-circuited by the cursor (seq ≤ last) or
    // rejected by the unique key, the batch is not applied twice.
    expect(body.applied).toBe(0);
    expect(body.deduped + body.ignored).toBe(1);
    expect(getCursor(db, store_id)).toBe(1);
    const count = db.query('SELECT COUNT(*) AS c FROM edge_sync_events WHERE store_id = ?').get(store_id) as any;
    expect(count.c).toBe(1); // stored once
  });

  it('ignores events at or below the cursor (out-of-order safe)', async () => {
    const { api_key } = await register();
    await signedRequest(api_key, 'POST', '/api/pos/events', { pos_hash: POS_HASH, sent_at: '', events: [sessionEvent(5, 'session.opened', 's5')] });
    const res = await signedRequest(api_key, 'POST', '/api/pos/events', { pos_hash: POS_HASH, sent_at: '', events: [sessionEvent(3, 'session.opened', 's3')] });
    const body = await res.json();
    expect(body.ignored).toBe(1);
    expect(body.applied).toBe(0);
  });

  it('rejects an unsigned request with 401', async () => {
    await register();
    const res = await app.handle(new Request(`${BASE}/api/pos/events`, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Flo-POS-Hash': POS_HASH },
      body: JSON.stringify({ pos_hash: POS_HASH, events: [] }),
    }));
    expect(res.status).toBe(401);
  });

  it('rejects a tampered body (hash/signature mismatch) with 401', async () => {
    const { api_key } = await register();
    const path = '/api/pos/events';
    const good = JSON.stringify({ pos_hash: POS_HASH, sent_at: '', events: [] });
    const headers = buildSignedHeaders(api_key, POS_HASH, 'POST', path, good);
    const res = await app.handle(new Request(`${BASE}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify({ pos_hash: POS_HASH, sent_at: '', events: [sessionEvent(9, 'session.opened', 'x')] }), // tampered
    }));
    expect(res.status).toBe(401);
  });
});

describe('commands — poll + result', () => {
  it('polls a queued command and acks its result', async () => {
    const { api_key } = await register();
    // queue via the internal helper
    const q = await app.handle(new Request(`${BASE}/internal/commands`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pos_hash: POS_HASH, cmd: 'menu.refresh', payload: { since: 0 } }),
    }));
    const { id } = await q.json();
    expect(id).toStartWith('cmd_');

    const poll = await signedRequest(api_key, 'GET', '/api/pos/commands?limit=5');
    const pb = await poll.json();
    expect(pb.commands.length).toBe(1);
    expect(pb.commands[0].cmd).toBe('menu.refresh');
    expect(pb.commands[0].payload.since).toBe(0);

    const ack = await signedRequest(api_key, 'POST', `/api/pos/commands/${id}/result`, { status: 'ok', result: { refreshed: true } });
    expect(ack.status).toBe(200);

    // once acked, it is no longer returned by the poll
    const poll2 = await signedRequest(api_key, 'GET', '/api/pos/commands?limit=5');
    expect((await poll2.json()).commands.length).toBe(0);
  });

  it('acking an unknown command id is 404', async () => {
    const { api_key } = await register();
    const res = await signedRequest(api_key, 'POST', '/api/pos/commands/cmd_nope/result', { status: 'ok' });
    expect(res.status).toBe(404);
  });
});
