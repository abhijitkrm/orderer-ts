// Pre-shared-key authentication: a challenge-response handshake proves both
// sides know the cluster key, and every later frame carries a MAC over
// (direction, sequence, kind, body). Authentication and integrity, not
// encryption. Same construction as orderer-rust's auth.rs.

import { createHmac, randomBytes, timingSafeEqual } from "crypto";

export const MAC_LEN = 16;
export const NONCE_LEN = 16;

export function hmac(key: Uint8Array, ...parts: Uint8Array[]): Buffer {
  const h = createHmac("sha256", key);
  for (const p of parts) h.update(p);
  return h.digest();
}

/// HMAC(key, label | parts...).
export function handshake(key: Uint8Array, label: string, ...parts: Uint8Array[]): Buffer {
  return hmac(key, Buffer.from(label), ...parts);
}

export const nonce = (): Buffer => randomBytes(NONCE_LEN);

export function ctEq(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && timingSafeEqual(a, b);
}

/// MACs the frames of one authenticated connection (both directions).
export class Session {
  private sendSeq = 0n;
  private recvSeq = 0n;
  constructor(private readonly key: Uint8Array, private readonly sendDir: number) {}

  private mac(dir: number, seq: bigint, kind: number, body: Uint8Array): Buffer {
    const head = Buffer.alloc(10);
    head[0] = dir;
    head.writeBigUInt64LE(seq, 1);
    head[9] = kind;
    return hmac(this.key, head, body).subarray(0, MAC_LEN);
  }

  seal(kind: number, body: Uint8Array): Buffer {
    const m = this.mac(this.sendDir, this.sendSeq, kind, body);
    this.sendSeq++;
    return m;
  }

  /// Verify a frame; a rejection does not advance the sequence.
  open(kind: number, body: Uint8Array, tag: Uint8Array): boolean {
    if (!ctEq(this.mac(1 - this.sendDir, this.recvSeq, kind, body), tag)) return false;
    this.recvSeq++;
    return true;
  }
}
