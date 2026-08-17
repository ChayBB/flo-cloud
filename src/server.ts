// FloPOS cloud edge-sync receiver (prototype) — Bun + Elysia + PostgreSQL.
// Implements docs/openapi/flopos-cloud.yaml derived from the FloROS edge.
import { Elysia } from 'elysia';
import {
  connect, initSchema, registerStore, storeByHash, claimNonce,
  applyEvents, getCursor, pollCommands, ackCommand, queueCommand, type Sql,
} from './db';
import { verifySignature } from './sign';

export function createApp(sql: Sql) {
  // rawBody is captured once per request (signatures are over exact bytes, so we
  // must not re-serialize the parsed body). Handlers parse JSON from rawBody.
  const app = new Elysia()
    .decorate('sql', sql)
    .derive(async ({ request }) => {
      let rawBody = '';
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        try { rawBody = await request.text(); } catch { rawBody = ''; }
      }
      return { rawBody };
    });

  const json = (raw: string) => { try { return raw ? JSON.parse(raw) : {}; } catch { return null; } };
  const signedPathOf = (request: Request) => { const u = new URL(request.url); return u.pathname + u.search; };

  // Resolve + verify a signed request. Returns the store, or {error,status}.
  async function authed(request: Request, rawBody: string, headers: Record<string, string | undefined>) {
    const posHash = headers['x-flo-pos-hash'];
    if (!posHash) return { error: 'Missing X-Flo-POS-Hash', status: 401 as const };
    const store = await storeByHash(sql, posHash);
    if (!store) return { error: 'Unknown store', status: 401 as const };
    const nonce = headers['x-flo-nonce'];
    if (!nonce || !(await claimNonce(sql, nonce))) return { error: 'Replay or missing nonce', status: 401 as const };
    const v = verifySignature({
      apiKey: store.api_key, method: request.method, signedPath: signedPathOf(request), body: rawBody, headers,
    });
    if (!v.ok) return { error: v.error, status: v.status as 401 };
    return { store };
  }

  app.post('/api/pos/register', async ({ rawBody, headers, set }) => {
    const posHash = headers['x-flo-pos-hash'];
    const body = json(rawBody);
    if (!posHash || !body || !body.business) { set.status = 400; return { error: 'pos_hash header and business are required' }; }
    const store = await registerStore(sql, posHash, body.business);
    return { store_id: store.store_id, api_key: store.api_key };
  });

  app.post('/api/pos/events', async ({ request, rawBody, headers, set }) => {
    const auth = await authed(request, rawBody, headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    const body = json(rawBody);
    if (!body || !Array.isArray(body.events)) { set.status = 400; return { error: 'events[] required' }; }
    const r = await applyEvents(sql, auth.store.store_id, body.events);
    return { ok: true, applied_seq: r.appliedSeq, applied: r.applied, deduped: r.deduped, ignored: r.ignored };
  });

  app.post('/api/pos/heartbeat', async ({ request, rawBody, headers, set }) => {
    const auth = await authed(request, rawBody, headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    return { ok: true, features: {} };
  });

  app.get('/api/pos/commands', async ({ request, headers, query, set }) => {
    const auth = await authed(request, '', headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    const limit = Math.min(50, Math.max(1, Number(query.limit ?? 5) || 5));
    return { commands: await pollCommands(sql, auth.store.store_id, limit) };
  });

  app.post('/api/pos/commands/:id/result', async ({ request, rawBody, headers, params, set }) => {
    const auth = await authed(request, rawBody, headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    const okAck = await ackCommand(sql, auth.store.store_id, params.id, json(rawBody) ?? {});
    if (!okAck) { set.status = 404; return { error: 'Unknown command id' }; }
    return { ok: true };
  });

  // Test/admin helper: queue a command (not part of the public edge contract).
  app.post('/internal/commands', async ({ rawBody, set }) => {
    const b = json(rawBody);
    if (!b?.pos_hash || !b?.cmd) { set.status = 400; return { error: 'pos_hash and cmd required' }; }
    const store = await storeByHash(sql, b.pos_hash);
    if (!store) { set.status = 404; return { error: 'Unknown store' }; }
    return { id: await queueCommand(sql, store.store_id, b.cmd, b.payload) };
  });

  return app;
}

export { getCursor };

if (import.meta.main) {
  const sql = connect();
  await initSchema(sql);
  const port = Number(process.env.PORT ?? 8787);
  createApp(sql).listen(port);
  console.log(`[flo-cloud] edge-sync receiver on http://localhost:${port} (PostgreSQL)`);
}
