// FloPOS cloud edge-sync receiver (prototype) — Bun + Elysia.
// Implements docs/openapi/flopos-cloud.yaml derived from the FloROS edge.
import { Elysia } from 'elysia';
import { Database } from 'bun:sqlite';
import {
  openDb, registerStore, storeByHash, claimNonce,
  applyEvents, getCursor, pollCommands, ackCommand, queueCommand,
} from './db';
import { verifySignature } from './sign';

export function createApp(db: Database = openDb('flo-cloud.db')) {
  // rawBody is captured once per request (signatures are over exact bytes, so we
  // must not re-serialize the parsed body). Handlers parse JSON from rawBody.
  const app = new Elysia()
    .decorate('db', db)
    .derive(async ({ request }) => {
      let rawBody = '';
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        try { rawBody = await request.text(); } catch { rawBody = ''; }
      }
      return { rawBody };
    });

  const json = (raw: string) => { try { return raw ? JSON.parse(raw) : {}; } catch { return null; } };
  const signedPathOf = (request: Request) => { const u = new URL(request.url); return u.pathname + u.search; };

  // Resolve + verify a signed request. Returns the store, or a {error,status}.
  function authed(request: Request, rawBody: string, headers: Record<string, string | undefined>) {
    const posHash = headers['x-flo-pos-hash'];
    if (!posHash) return { error: 'Missing X-Flo-POS-Hash', status: 401 as const };
    const store = storeByHash(db, posHash);
    if (!store) return { error: 'Unknown store', status: 401 as const };
    const nonce = headers['x-flo-nonce'];
    if (!nonce || !claimNonce(db, nonce)) return { error: 'Replay or missing nonce', status: 401 as const };
    const v = verifySignature({
      apiKey: store.api_key, method: request.method, signedPath: signedPathOf(request), body: rawBody, headers,
    });
    if (!v.ok) return { error: v.error, status: v.status as 401 };
    return { store };
  }

  // ── Onboarding (unauthenticated) ──────────────────────────────────────────
  app.post('/api/pos/register', ({ rawBody, headers, set }) => {
    const posHash = headers['x-flo-pos-hash'];
    const body = json(rawBody);
    if (!posHash || !body || !body.business) { set.status = 400; return { error: 'pos_hash header and business are required' }; }
    const store = registerStore(db, posHash, body.business);
    return { store_id: store.store_id, api_key: store.api_key };
  });

  // ── Event sink v2 ─────────────────────────────────────────────────────────
  app.post('/api/pos/events', ({ request, rawBody, headers, set }) => {
    const auth = authed(request, rawBody, headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    const body = json(rawBody);
    if (!body || !Array.isArray(body.events)) { set.status = 400; return { error: 'events[] required' }; }
    const r = applyEvents(db, auth.store.store_id, body.events);
    return { ok: true, applied_seq: r.appliedSeq, applied: r.applied, deduped: r.deduped, ignored: r.ignored };
  });

  // ── Heartbeat ─────────────────────────────────────────────────────────────
  app.post('/api/pos/heartbeat', ({ request, rawBody, headers, set }) => {
    const auth = authed(request, rawBody, headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    return { ok: true, features: {} };
  });

  // ── Command poll (HTTP fallback) ──────────────────────────────────────────
  app.get('/api/pos/commands', ({ request, headers, query, set }) => {
    const auth = authed(request, '', headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    const limit = Math.min(50, Math.max(1, Number(query.limit ?? 5) || 5));
    return { commands: pollCommands(db, auth.store.store_id, limit) };
  });

  // ── Command result ────────────────────────────────────────────────────────
  app.post('/api/pos/commands/:id/result', ({ request, rawBody, headers, params, set }) => {
    const auth = authed(request, rawBody, headers);
    if ('error' in auth) { set.status = auth.status; return { error: auth.error }; }
    const body = json(rawBody) ?? {};
    const okAck = ackCommand(db, auth.store.store_id, params.id, body);
    if (!okAck) { set.status = 404; return { error: 'Unknown command id' }; }
    return { ok: true };
  });

  // Test/admin helper: queue a command (not part of the public edge contract).
  app.post('/internal/commands', ({ rawBody, set }) => {
    const b = json(rawBody);
    if (!b?.pos_hash || !b?.cmd) { set.status = 400; return { error: 'pos_hash and cmd required' }; }
    const store = storeByHash(db, b.pos_hash);
    if (!store) { set.status = 404; return { error: 'Unknown store' }; }
    return { id: queueCommand(db, store.store_id, b.cmd, b.payload) };
  });

  return app;
}

// Expose cursor read for tests/observability.
export { getCursor };

if (import.meta.main) {
  const port = Number(process.env.PORT ?? 8787);
  createApp().listen(port);
  console.log(`[flo-cloud] edge-sync receiver on http://localhost:${port}`);
}
