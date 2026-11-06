// Framing shared by the replica transport and the client protocol:
// len u32 | crc32c u32 | kind u8 | body [| 16-byte MAC once the connection is
// authenticated]. len and crc cover kind, body and MAC. Byte-compatible with
// orderer-rust, orderer-cpp and orderer-go.

import { MAC_LEN, Session } from "./auth";
import { crc32c } from "./crc";

export const MAX_NET_FRAME = 64 << 20;

export class NetError extends Error {}

export interface Frame { kind: number; body: Buffer }

/// A frame (sealed when `sess` is set), ready to write.
export function putFrame(kind: number, body: Uint8Array, sess?: Session): Buffer {
  const extra = sess ? MAC_LEN : 0;
  const out = Buffer.allocUnsafe(9 + body.length + extra);
  out[8] = kind;
  out.set(body, 9);
  if (sess) sess.seal(kind, body).copy(out, 9 + body.length);
  out.writeUInt32LE(1 + body.length + extra, 0);
  out.writeUInt32LE(crc32c(out, 8, out.length), 4);
  return out;
}

/// Incremental frame parser: push socket chunks, receive verified frames.
/// A framing, checksum or authentication failure throws NetError from push().
export class FrameReader {
  /// Set once the connection is authenticated; applies from the next frame.
  sess?: Session;
  private parts: Buffer[] = [];
  private have = 0;
  private need = 0; // body length being waited for; 0 = waiting for a header
  private crc = 0;

  private take(n: number): Buffer {
    this.have -= n;
    const first = this.parts[0];
    if (first.length >= n) {
      if (first.length === n) this.parts.shift();
      else this.parts[0] = first.subarray(n);
      return first.subarray(0, n);
    }
    const out = Buffer.allocUnsafe(n);
    let o = 0;
    while (o < n) {
      const p = this.parts[0];
      const k = Math.min(p.length, n - o);
      p.copy(out, o, 0, k);
      o += k;
      if (k === p.length) this.parts.shift();
      else this.parts[0] = p.subarray(k);
    }
    return out;
  }

  push(chunk: Buffer, onFrame: (f: Frame) => void): void {
    this.parts.push(chunk);
    this.have += chunk.length;
    for (;;) {
      if (this.need === 0) {
        if (this.have < 8) return;
        const h = this.take(8);
        const len = h.readUInt32LE(0);
        if (len === 0 || len > MAX_NET_FRAME) throw new NetError("frame length");
        this.need = len;
        this.crc = h.readUInt32LE(4);
      }
      if (this.have < this.need) return;
      const body = this.take(this.need);
      this.need = 0;
      if (crc32c(body) !== this.crc) throw new NetError("frame checksum");
      let f: Frame = { kind: body[0], body: body.subarray(1) };
      if (this.sess) {
        if (f.body.length < MAC_LEN) throw new NetError("frame authentication");
        const n = f.body.length - MAC_LEN;
        if (!this.sess.open(f.kind, f.body.subarray(0, n), f.body.subarray(n))) throw new NetError("frame authentication");
        f = { kind: f.kind, body: f.body.subarray(0, n) };
      }
      onFrame(f);
    }
  }
}

export function parseAddr(s: string): { host: string; port: number } {
  const c = s.lastIndexOf(":");
  if (c < 0) throw new NetError(`bad address ${s}`);
  const host = s.slice(0, c);
  const port = Number(s.slice(c + 1));
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new NetError(`bad address ${s}`);
  return { host: host === "localhost" ? "127.0.0.1" : host, port };
}
