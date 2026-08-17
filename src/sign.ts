// HMAC request signing — mirrors the FloROS edge (cloud-sync.ts buildSignedHeaders).
//   signatureBase = METHOD "\n" signedPath "\n" timestamp "\n" nonce "\n" sha256(body)
//   X-Flo-Signature = sha256=<hex HMAC-SHA256(api_key, signatureBase)>
import { createHash, createHmac, timingSafeEqual } from 'node:crypto';

export function sha256Hex(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

export function hmacHex(secret: string, value: string): string {
  return createHmac('sha256', secret).update(value, 'utf8').digest('hex');
}

export interface SignedHeaders {
  Authorization: string;
  'X-Flo-POS-Hash': string;
  'X-Flo-Timestamp': string;
  'X-Flo-Nonce': string;
  'X-Flo-Body-SHA256': string;
  'X-Flo-Signature': string;
}

/** Build the headers a client sends (also used by tests as the edge would). */
export function buildSignedHeaders(
  apiKey: string,
  posHash: string,
  method: string,
  signedPath: string,
  body: string,
): SignedHeaders {
  const timestamp = new Date().toISOString();
  const nonce = crypto.randomUUID();
  const bodyHash = sha256Hex(body);
  const base = [method.toUpperCase(), signedPath, timestamp, nonce, bodyHash].join('\n');
  return {
    Authorization: `Bearer ${apiKey}`,
    'X-Flo-POS-Hash': posHash,
    'X-Flo-Timestamp': timestamp,
    'X-Flo-Nonce': nonce,
    'X-Flo-Body-SHA256': bodyHash,
    'X-Flo-Signature': `sha256=${hmacHex(apiKey, base)}`,
  };
}

const MAX_SKEW_MS = 5 * 60 * 1000;

export type VerifyResult =
  | { ok: true }
  | { ok: false; status: number; error: string };

/**
 * Verify a signed request. Checks: body hash, timestamp skew, signature. Nonce
 * replay is enforced by the caller (needs storage). Returns 401 on any failure.
 */
export function verifySignature(input: {
  apiKey: string;
  method: string;
  signedPath: string;
  body: string;
  headers: Record<string, string | undefined>;
}): VerifyResult {
  const h = (name: string) => input.headers[name.toLowerCase()] ?? input.headers[name];
  const timestamp = h('X-Flo-Timestamp');
  const nonce = h('X-Flo-Nonce');
  const bodyHash = h('X-Flo-Body-SHA256');
  const signature = h('X-Flo-Signature');
  if (!timestamp || !nonce || !bodyHash || !signature) {
    return { ok: false, status: 401, error: 'Missing signing headers' };
  }

  if (sha256Hex(input.body) !== bodyHash) {
    return { ok: false, status: 401, error: 'Body hash mismatch' };
  }

  const ts = Date.parse(timestamp);
  if (Number.isNaN(ts) || Math.abs(Date.now() - ts) > MAX_SKEW_MS) {
    return { ok: false, status: 401, error: 'Timestamp outside allowed skew' };
  }

  const base = [input.method.toUpperCase(), input.signedPath, timestamp, nonce, bodyHash].join('\n');
  const expected = `sha256=${hmacHex(input.apiKey, base)}`;
  const a = Buffer.from(expected);
  const b = Buffer.from(signature);
  if (a.length !== b.length || !timingSafeEqual(a, b)) {
    return { ok: false, status: 401, error: 'Invalid signature' };
  }
  return { ok: true };
}
