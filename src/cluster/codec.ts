// Binary encodings shared with orderer-rust (codec.rs), orderer-cpp and
// orderer-go: protocol messages, WAL frame bodies and command-batch log entry
// payloads. Little-endian. Decoders never read out of bounds; they throw
// CodecError on malformed input.
//
// Indexes, order ids, prices and quantities are JS numbers (matcher-ts's
// range, exact to 2^53). A wire value outside that range cannot be
// represented and is rejected as a CodecError; session ids are BigInt.

import { Disk, Entry, Msg, NONE, Persist } from "../consensus";
import { Command, OType, Side, Tif } from "../matcher/types";

export class CodecError extends Error {}

export const MAX_ENTRY = 16 << 20;
export const CMD_BYTES = 32;
export const TAG_BATCH = 1, TAG_SESSION_BATCH = 2;
const TWO32 = 4294967296;
const HI_LIMIT = 1 << 21; // hi word < 2^21  =>  value < 2^53

export type CmdSym = [number, Command];
export interface SessionKey { client: bigint; seq: bigint }

/// A bounds-checked cursor over a buffer.
export class Reader {
  private pos = 0;
  constructor(readonly b: Uint8Array) {}
  get remaining(): number { return this.b.length - this.pos; }
  bytes(n: number): Uint8Array {
    if (this.remaining < n) throw new CodecError("truncated input");
    const r = this.b.subarray(this.pos, this.pos + n);
    this.pos += n;
    return r;
  }
  private view(n: number): Buffer {
    if (this.remaining < n) throw new CodecError("truncated input");
    const o = this.pos;
    this.pos += n;
    return Buffer.from(this.b.buffer, this.b.byteOffset + o, n);
  }
  u8(): number { return this.view(1)[0]; }
  u32(): number { return this.view(4).readUInt32LE(0); }
  /// A u64 that must fit a JS number.
  u64(): number {
    const v = this.view(8);
    const hi = v.readUInt32LE(4);
    if (hi >= HI_LIMIT) throw new CodecError("value beyond 2^53");
    return hi * TWO32 + v.readUInt32LE(0);
  }
  u64big(): bigint { return this.view(8).readBigUInt64LE(0); }
  flag(): boolean {
    const v = this.u8();
    if (v > 1) throw new CodecError("flag");
    return v === 1;
  }
  finish(): void { if (this.remaining) throw new CodecError("trailing bytes"); }
}

/// A growable output buffer.
export class Out {
  buf: Buffer;
  len = 0;
  constructor(cap = 64) { this.buf = Buffer.allocUnsafe(cap); }
  private room(n: number): number {
    if (this.len + n > this.buf.length) {
      const nb = Buffer.allocUnsafe(Math.max(this.buf.length * 2, this.len + n));
      this.buf.copy(nb, 0, 0, this.len);
      this.buf = nb;
    }
    const o = this.len;
    this.len += n;
    return o;
  }
  // `room` may replace `buf`: take the offset first, then read the field.
  u8(v: number): this { const o = this.room(1); this.buf[o] = v; return this; }
  u32(v: number): this { const o = this.room(4); this.buf.writeUInt32LE(v >>> 0, o); return this; }
  u64(v: number): this { const o = this.room(8); put64(this.buf, o, v); return this; }
  u64big(v: bigint): this { const o = this.room(8); this.buf.writeBigUInt64LE(v, o); return this; }
  bool(v: boolean): this { return this.u8(v ? 1 : 0); }
  bytes(b: Uint8Array): this { const o = this.room(b.length); this.buf.set(b, o); return this; }
  done(): Buffer { return this.buf.subarray(0, this.len); }
}

/// Write a number as a little-endian 64-bit two's complement integer.
export function put64(b: Buffer, o: number, v: number): void {
  b.writeUInt32LE(v >>> 0, o);
  b.writeUInt32LE(Math.floor(v / TWO32) >>> 0, o + 4);
}

export function get64(b: Buffer, o: number, signed: boolean): number {
  const lo = b.readUInt32LE(o);
  if (signed) {
    const hi = b.readInt32LE(o + 4);
    if (hi >= HI_LIMIT || hi < -HI_LIMIT) throw new CodecError("value beyond 2^53");
    return hi * TWO32 + lo;
  }
  const hi = b.readUInt32LE(o + 4);
  if (hi >= HI_LIMIT) throw new CodecError("value beyond 2^53");
  return hi * TWO32 + lo;
}

function putEntries(o: Out, es: Entry[]): void {
  o.u32(es.length);
  for (const e of es) o.u32(e.term).u32(e.data.length).bytes(e.data);
}

function getEntries(r: Reader, own: boolean): Entry[] {
  const n = r.u32();
  // Each entry is at least 8 bytes; reject counts the input cannot hold.
  if (n > Math.floor(r.remaining / 8)) throw new CodecError("entry count");
  const out: Entry[] = [];
  for (let i = 0; i < n; i++) {
    const term = r.u32();
    const l = r.u32();
    if (l > MAX_ENTRY) throw new CodecError("entry too large");
    let data = r.bytes(l);
    if (own) data = new Uint8Array(data);
    out.push({ term, data });
  }
  return out;
}

export function encodeMsg(m: Msg, o = new Out()): Out {
  switch (m.kind) {
    case "vote": o.u8(1).u32(m.term).u64(m.lastIndex).u32(m.lastTerm).bool(m.pre); break;
    case "votereply": o.u8(2).u32(m.term).bool(m.granted).bool(m.pre); break;
    case "append": o.u8(3).u32(m.term).u64(m.prevIndex).u32(m.prevTerm).u64(m.commit); putEntries(o, m.entries); break;
    case "appendreply": o.u8(4).u32(m.term).bool(m.success).u64(m.index); break;
  }
  return o;
}

function readMsg(r: Reader): Msg {
  switch (r.u8()) {
    case 1: return { kind: "vote", term: r.u32(), lastIndex: r.u64(), lastTerm: r.u32(), pre: r.flag() };
    case 2: return { kind: "votereply", term: r.u32(), granted: r.flag(), pre: r.flag() };
    case 3: {
      const term = r.u32(), prevIndex = r.u64(), prevTerm = r.u32(), commit = r.u64();
      return { kind: "append", term, prevIndex, prevTerm, commit, entries: getEntries(r, false) };
    }
    case 4: return { kind: "appendreply", term: r.u32(), success: r.flag(), index: r.u64() };
    default: throw new CodecError("message tag");
  }
}

/// Decode exactly one message.
export function decodeMsg(b: Uint8Array): Msg {
  const r = new Reader(b);
  const m = readMsg(r);
  r.finish();
  return m;
}

/// A WAL frame body: one atomic consensus write (the token is not stored).
export function encodePersist(p: Persist, o = new Out()): Out {
  o.u32(p.term).u8(p.votedFor === NONE ? 255 : p.votedFor);
  if (p.compact) o.u8(1).u64(p.compact.index).u32(p.compact.term);
  else o.u8(0);
  o.u64(p.from);
  putEntries(o, p.entries);
  return o;
}

export function decodePersist(b: Uint8Array): Persist {
  const r = new Reader(b);
  const term = r.u32();
  const v = r.u8();
  const votedFor = v === 255 ? NONE : v;
  let compact: Persist["compact"] = null;
  switch (r.u8()) {
    case 0: break;
    case 1: compact = { index: r.u64(), term: r.u32() }; break;
    default: throw new CodecError("compact flag");
  }
  const from = r.u64();
  if (from === 0) throw new CodecError("write starts at index 0");
  const entries = getEntries(r, true);
  r.finish();
  return { token: 0, term, votedFor, from, entries, compact };
}

/// Apply one write to a recovered disk image (the protocol core's Disk
/// rules). Writes must be applied in issue order.
export function applyPersist(d: Disk, p: Persist): void {
  d.term = p.term;
  d.votedFor = p.votedFor;
  if (p.compact && p.compact.index > d.base) {
    const drop = Math.min(p.compact.index - d.base, d.log.length);
    d.log = d.log.slice(drop);
    d.base = p.compact.index;
    d.baseTerm = p.compact.term;
  }
  const keep = p.from - 1 - d.base;
  if (keep < d.log.length) d.log.length = keep;
  for (const e of p.entries) d.log.push(e);
}

// ---- command batches: the payload of a log entry ----

/// One command as the journal record's bytes 8..40 (symbol, kind, side,
/// otype, tif, order id, price, qty) without iseq, checksum or padding.
export function putCmd32(r: Buffer, o: number, sym: number, c: Command): void {
  r.fill(0, o, o + CMD_BYTES);
  r.writeUInt32LE(sym, o);
  put64(r, o + 8, c.orderId);
  if (c.kind === "new") {
    r[o + 4] = 1; r[o + 5] = c.side; r[o + 6] = c.otype; r[o + 7] = c.tif;
    put64(r, o + 16, c.price);
    put64(r, o + 24, c.qty);
  } else if (c.kind === "cancel") {
    r[o + 4] = 2;
  } else {
    r[o + 4] = 3;
    put64(r, o + 16, c.price);
    put64(r, o + 24, c.qty);
  }
}

/// `n` commands: 0x01 | n u32 | n*32 bytes; the session form is
/// 0x02 | client u64 | seq u64 | n | ...
export function encodeBatch(cmds: ReadonlyArray<CmdSym>, session?: SessionKey): Buffer {
  const head = session ? 1 + 16 + 4 : 1 + 4;
  const b = Buffer.allocUnsafe(head + cmds.length * CMD_BYTES);
  let o = 0;
  if (session) {
    b[o++] = TAG_SESSION_BATCH;
    b.writeBigUInt64LE(session.client, o); o += 8;
    b.writeBigUInt64LE(session.seq, o); o += 8;
  } else b[o++] = TAG_BATCH;
  b.writeUInt32LE(cmds.length, o); o += 4;
  for (let i = 0; i < cmds.length; i++) putCmd32(b, o + i * CMD_BYTES, cmds[i][0], cmds[i][1]);
  return b;
}

/// The session a payload belongs to, if any, without decoding commands.
export function sessionOf(d: Uint8Array): SessionKey | undefined {
  if (d.length < 17 || d[0] !== TAG_SESSION_BATCH) return undefined;
  const b = Buffer.from(d.buffer, d.byteOffset, d.length);
  return { client: b.readBigUInt64LE(1), seq: b.readBigUInt64LE(9) };
}

/// Decode a plain or session-tagged payload. Empty is a leader's no-op.
export function decodeBatch(d: Uint8Array): CmdSym[] {
  if (d.length === 0) return [];
  const r = new Reader(d);
  switch (r.u8()) {
    case TAG_BATCH: break;
    case TAG_SESSION_BATCH: r.u64big(); r.u64big(); break;
    default: throw new CodecError("entry tag");
  }
  const count = r.u32();
  const rem = r.remaining;
  if (count !== Math.floor(rem / CMD_BYTES) || rem % CMD_BYTES !== 0) throw new CodecError("batch length");
  const out: CmdSym[] = [];
  for (let i = 0; i < count; i++) {
    const raw = r.bytes(CMD_BYTES);
    const b = Buffer.from(raw.buffer, raw.byteOffset, CMD_BYTES);
    const sym = b.readUInt32LE(0);
    const orderId = get64(b, 8, false);
    switch (b[4]) {
      case 1:
        if (b[5] > 1 || b[6] > 1 || b[7] > 3) throw new CodecError("command");
        out.push([sym, { kind: "new", orderId, side: b[5] as Side, otype: b[6] as OType, price: get64(b, 16, true), qty: get64(b, 24, false), tif: b[7] as Tif }]);
        break;
      case 2: out.push([sym, { kind: "cancel", orderId }]); break;
      case 3: out.push([sym, { kind: "replace", orderId, price: get64(b, 16, true), qty: get64(b, 24, false) }]); break;
      default: throw new CodecError("command");
    }
  }
  return out;
}
