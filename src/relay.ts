// WSS relay hub — the primary cloud→edge push channel (`/api/pos/relay`).
// Tracks one or more live sockets per store and pushes command frames the moment
// they're queued; the HTTP `/api/pos/commands` poll is the fallback when no
// socket is connected. Frame shapes mirror the edge client (cloud-sync.ts):
//   cloud→edge: { type:'command', id, cmd, payload:{ version:1, payload, correlation_id } }
//               { type:'heartbeat_ack', features }
//   edge→cloud: { type:'heartbeat', ... }  { type:'result', id, ...body }
import { ackCommand, type Sql } from './db';

export interface RelaySocket {
  send(data: string): void;
  readyState: number;
  data: Record<string, any>;
}

export interface QueuedCommand {
  id: string;
  cmd: string;
  payload?: unknown;
  correlation_id?: string;
}

export class RelayHub {
  private conns = new Map<string, Set<RelaySocket>>();

  register(storeId: string, ws: RelaySocket): void {
    let set = this.conns.get(storeId);
    if (!set) { set = new Set(); this.conns.set(storeId, set); }
    set.add(ws);
  }

  unregister(storeId: string, ws: RelaySocket): void {
    const set = this.conns.get(storeId);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) this.conns.delete(storeId);
  }

  isConnected(storeId: string): boolean {
    return (this.conns.get(storeId)?.size ?? 0) > 0;
  }

  /** Push a command to every live socket for the store. Returns how many got it. */
  pushCommand(storeId: string, command: QueuedCommand): number {
    const set = this.conns.get(storeId);
    if (!set || set.size === 0) return 0;
    const frame = JSON.stringify({
      type: 'command',
      id: command.id,
      cmd: command.cmd,
      payload: { version: 1, payload: command.payload ?? {}, correlation_id: command.correlation_id ?? command.id },
    });
    let sent = 0;
    for (const ws of set) {
      try { ws.send(frame); sent++; } catch { /* drop dead socket on next close */ }
    }
    return sent;
  }

  /** Handle one inbound edge→cloud frame (string, Buffer, or already-parsed). */
  async handleMessage(sql: Sql, storeId: string, ws: RelaySocket, raw: unknown): Promise<void> {
    let frame: any;
    if (raw && typeof raw === 'object' && !(raw instanceof Uint8Array)) {
      frame = raw;
    } else {
      try { frame = JSON.parse(typeof raw === 'string' ? raw : (raw as any).toString()); } catch { return; }
    }
    if (!frame || typeof frame !== 'object') return;

    if (frame.type === 'heartbeat') {
      // Ack with the current feature flags (empty for the prototype).
      try { ws.send(JSON.stringify({ type: 'heartbeat_ack', features: {} })); } catch { /* ignore */ }
      return;
    }
    if (frame.type === 'result' && frame.id) {
      const { type: _t, id, ...body } = frame;
      await ackCommand(sql, storeId, id, body);
      return;
    }
  }
}
