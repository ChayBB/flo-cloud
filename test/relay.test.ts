import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { createApp } from '../src/server';
import { RelayHub } from '../src/relay';
import { connect, initSchema, resetSchema, registerStore, type Sql } from '../src/db';
import { buildSignedHeaders } from '../src/sign';

const TEST_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/flo_cloud_test';
const POS_HASH = 'relay-pos-1';

let sql: Sql;
let server: ReturnType<ReturnType<typeof createApp>['listen']>;
let baseHttp: string;
let baseWs: string;
let apiKey: string;

/** Open a WS with the signed handshake headers; resolve once OPEN. */
function openRelay(headers: Record<string, string>) {
  const ws = new WebSocket(`${baseWs}/api/pos/relay`, { headers } as any);
  const messages: any[] = [];
  ws.addEventListener('message', (e: any) => { try { messages.push(JSON.parse(e.data)); } catch { /* ignore */ } });
  const opened = new Promise<void>((resolve, reject) => {
    ws.addEventListener('open', () => resolve());
    ws.addEventListener('error', () => reject(new Error('ws error')));
    ws.addEventListener('close', (e: any) => reject(new Error(`closed ${e.code}`)));
  });
  return { ws, messages, opened };
}

function signedRelayHeaders() {
  return buildSignedHeaders(apiKey, POS_HASH, 'GET', '/api/pos/relay', '') as unknown as Record<string, string>;
}

async function queueCommand(cmd: string, payload: unknown) {
  const res = await fetch(`${baseHttp}/internal/commands`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pos_hash: POS_HASH, cmd, payload }),
  });
  return res.json();
}

const waitFor = async (pred: () => boolean | Promise<boolean>, ms = 3000) => {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (await pred()) return;
    await Bun.sleep(20);
  }
  throw new Error('timeout waiting for condition');
};

beforeAll(async () => {
  sql = connect(TEST_URL);
  await initSchema(sql);
  await resetSchema(sql);
  const app = createApp(sql, new RelayHub());
  server = app.listen(0);
  const port = (server as any).server?.port ?? (server as any).port;
  baseHttp = `http://localhost:${port}`;
  baseWs = `ws://localhost:${port}`;
  const store = await registerStore(sql, POS_HASH, { name: 'Relay Cafe' });
  apiKey = store.api_key;
});

afterAll(async () => { server?.stop?.(); await sql.end(); });

describe('WSS relay', () => {
  it('rejects a handshake with no signature (closes)', async () => {
    const ws = new WebSocket(`${baseWs}/api/pos/relay`, { headers: { 'X-Flo-POS-Hash': POS_HASH } } as any);
    const closed = new Promise<number>((res) => ws.addEventListener('close', (e: any) => res(e.code)));
    const code = await Promise.race([closed, Bun.sleep(2500).then(() => -1)]);
    expect(code).not.toBe(-1);         // it did close
    expect(ws.readyState).not.toBe(WebSocket.OPEN);
  });

  it('accepts a signed handshake, pushes a queued command, and acks the result', async () => {
    const { ws, messages, opened } = openRelay(signedRelayHeaders());
    await opened;

    const { id, pushed } = await queueCommand('menu.refresh', { since: 7 });
    expect(pushed).toBe(1); // delivered over the live socket

    await waitFor(() => messages.some((m) => m.type === 'command'));
    const frame = messages.find((m) => m.type === 'command');
    expect(frame.id).toBe(id);
    expect(frame.cmd).toBe('menu.refresh');
    expect(frame.payload.version).toBe(1);
    expect(frame.payload.payload.since).toBe(7);

    // edge replies with a result -> command becomes ACKED
    ws.send(JSON.stringify({ type: 'result', id, status: 'ok', result: { refreshed: true } }));
    await waitFor(async () => {
      const rows = await sql`SELECT status FROM edge_commands WHERE id = ${id}`;
      return rows[0]?.status === 'ACKED';
    });
    const rows = await sql`SELECT status, result FROM edge_commands WHERE id = ${id}`;
    expect(rows[0].status).toBe('ACKED');

    ws.close();
  });

  it('answers a heartbeat with heartbeat_ack', async () => {
    const { ws, messages, opened } = openRelay(signedRelayHeaders());
    await opened;
    await Bun.sleep(150); // let the server's async open() finish registering
    ws.send(JSON.stringify({ type: 'heartbeat', active_orders: 3 }));
    await waitFor(() => messages.some((m) => m.type === 'heartbeat_ack'));
    expect(messages.find((m) => m.type === 'heartbeat_ack').features).toBeDefined();
    ws.close();
  });
});
