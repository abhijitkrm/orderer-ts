//! Per-partition journals (spec/JOURNAL.md): naming, JSONL and binary
//! encodings, strict readers, and the asynchronous chunk writer whose I/O
//! runs on its own worker.

import * as fs from "fs";
import * as path from "path";
import { BookConfig } from "./matcher/book";
import { IndexKind } from "./matcher/priceindex";
import { CloseReason, Command, Event, OType, RejectReason, Side, Tif, eventCanonical } from "./matcher/types";
import { get, i64, indexName, parseCommand, sameBook, u64, writeCommand } from "./flat";


export type JournalFormat = "jsonl" | "binary";
export type JournalKind = "cmd" | "evt";

export const HEADER = 64, MAX_RECORD = 256, VERSION = 2;
/// Version-2 record sizes (1.2): the version-1 record + CRC-32C + 4 reserved bytes.
export const CMD_RECORD = 48, EVT_RECORD = 56, CMD_RECORD_V1 = 40, EVT_RECORD_V1 = 48;
const payloadOf = (k: JournalKind) => (k === "cmd" ? CMD_RECORD_V1 : EVT_RECORD_V1);
const recordSize = (k: JournalKind, version: number) => payloadOf(k) + (version === 1 ? 0 : 8);

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1;
    t[i] = c >>> 0;
  }
  return t;
})();

/// CRC-32C (Castagnoli, reflected 0x82F63B78), spec/JOURNAL.md §2.2.
export function crc32c(b: Uint8Array, from = 0, len = b.length - from): number {
  let c = 0xffffffff;
  for (let i = from; i < from + len; i++) c = CRC_TABLE[(c ^ b[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function bytesOf(v: DataView): Uint8Array {
  return new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
}

/// Seal a version-2 record at `o`: CRC-32C of the payload, then zeros.
function seal(v: DataView, o: number, payload: number): void {
  v.setUint32(o + payload, crc32c(bytesOf(v), o, payload), true);
  v.setUint32(o + payload + 4, 0, true);
}

function sealed(v: DataView, o: number, payload: number): boolean {
  return v.getUint32(o + payload, true) === crc32c(bytesOf(v), o, payload);
}

/// When the I/O worker fsyncs. Never observable (spec/PIPELINE.md §8).
export type FsyncPolicy =
  | { mode: "never" }
  | { mode: "everyN"; n: number; idleUs: number } // group commit at n pending records, or after idleUs with no more
  | { mode: "every"; intervalUs: number };

export const fsyncNever = (): FsyncPolicy => ({ mode: "never" });
export const fsyncEveryN = (n: number): FsyncPolicy => ({ mode: "everyN", n, idleUs: 200 });
export const fsyncEvery = (us: number): FsyncPolicy => ({ mode: "every", intervalUs: us });

export interface JournalConfig {
  dir: string;
  format: JournalFormat;
  fsync: FsyncPolicy;
  events: boolean; // event journals too (derived data)
  append: boolean; // reopen existing journals (after recovery)
}

/// Binary or JSONL; durable (fsync every 1024 records); event journals on.
export function journalConfig(dir: string, format: JournalFormat): JournalConfig {
  return { dir, format, fsync: fsyncEveryN(1024), events: true, append: false };
}

export class CorruptJournal extends Error {}

const corrupt = (p: string, d: string) => new CorruptJournal(`${p}: ${d}`);

const extOf = (f: JournalFormat) => (f === "jsonl" ? ".journal" : ".bin");

/// spec/JOURNAL.md §1: segment 0's file.
export function journalPath(dir: string, k: JournalKind, p: number, f: JournalFormat): string {
  return segmentPath(dir, k, p, 0, f);
}

/// spec/JOURNAL.md §1: the segment starting after cut `start`.
export function segmentPath(dir: string, k: JournalKind, p: number, start: number, f: JournalFormat): string {
  return path.join(dir, `${k}-${p}${start === 0 ? "" : "." + start}${extOf(f)}`);
}

export interface Segment {
  partition: number;
  start: number;
  path: string;
}

const UINT = /^[0-9]+$/;

/// Every `k` segment in `dir`, sorted by (partition, start).
export function listSegments(dir: string, k: JournalKind, f: JournalFormat): Segment[] {
  let names: string[];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  const prefix = k + "-", suffix = extOf(f);
  const out: Segment[] = [];
  for (const name of names) {
    if (!name.startsWith(prefix) || !name.endsWith(suffix) || name.length <= prefix.length + suffix.length) continue;
    const mid = name.slice(prefix.length, name.length - suffix.length);
    const dot = mid.indexOf(".");
    const ps = dot < 0 ? mid : mid.slice(0, dot), ss = dot < 0 ? "0" : mid.slice(dot + 1);
    if (!UINT.test(ps) || !UINT.test(ss)) continue;
    const p = Number(ps), start = Number(ss);
    if (p > 0xffffffff || !Number.isSafeInteger(start) || (dot >= 0 && start === 0)) continue;
    out.push({ partition: p, start, path: path.join(dir, name) });
  }
  return out.sort((a, b) => a.partition - b.partition || a.start - b.start);
}

/// spec/JOURNAL.md §6: checkpoint snapshot path for cut `n`.
export function checkpointPath(dir: string, n: number): string {
  return path.join(dir, `checkpoint-${n}.snap`);
}

/// Checkpoints with cut below `below`, ascending; `complete` = with sidecar.
export function listCheckpoints(dir: string, complete = true, below = Infinity): Array<{ cut: number; path: string }> {
  let names: string[];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  const out: Array<{ cut: number; path: string }> = [];
  for (const name of names) {
    const m = /^checkpoint-([0-9]+)\.snap$/.exec(name);
    if (m === null) continue;
    const n = Number(m[1]);
    if (!Number.isSafeInteger(n) || n >= below) continue;
    const p = path.join(dir, name);
    if (complete && !fs.existsSync(p + ".meta")) continue;
    out.push({ cut: n, path: p });
  }
  return out.sort((a, b) => a.cut - b.cut);
}

// ---- 64-bit helpers (numbers exact to 2^53) -------------------------------------------------

const TWO32 = 4294967296;

function setI64(v: DataView, o: number, n: number): void {
  const hi = Math.floor(n / TWO32);
  v.setUint32(o, n - hi * TWO32, true);
  v.setInt32(o + 4, hi | 0, true);
}

/// A u64/i64 field as a number, or undefined beyond 2^53.
function getI64(v: DataView, o: number, signed: boolean): number | undefined {
  const lo = v.getUint32(o, true);
  const hi = signed ? v.getInt32(o + 4, true) : v.getUint32(o + 4, true);
  const n = hi * TWO32 + lo;
  return Number.isSafeInteger(n) ? n : undefined;
}

// ---- encodings ---------------------------------------------------------------------------------

export function jsonlHeader(k: JournalKind, p: number, P: number, b: BookConfig): string {
  return `{"format":"orderer-journal/1","kind":"${k}","partition":${p},"partitions":${P},"pmin":${b.priceMin},"pmax":${b.priceMax},"max_orders":${b.maxOrders},"index":"${indexName(b.index)}"}\n`;
}

export function binaryHeader(k: JournalKind, p: number, P: number, b: BookConfig): Uint8Array {
  const h = new Uint8Array(HEADER);
  const v = new DataView(h.buffer);
  h.set([0x4f, 0x52, 0x44, 0x4a]); // "ORDJ"
  v.setUint16(4, VERSION, true);
  h[6] = k === "cmd" ? 1 : 2;
  h[7] = b.index === IndexKind.Tree ? 1 : 0;
  v.setUint32(8, p, true);
  v.setUint32(12, P, true);
  v.setUint32(16, recordSize(k, VERSION), true);
  setI64(v, 24, b.priceMin);
  setI64(v, 32, b.priceMax);
  setI64(v, 40, b.maxOrders);
  return h;
}

/// JSONL command record (no newline): canonical engine line + "iseq".
export function cmdLine(iseq: number, sym: number, c: Command): string {
  const l = writeCommand(c, sym);
  return `${l.slice(0, -1)},"iseq":${iseq}}`;
}

export function encodeCmd(iseq: number, sym: number, c: Command, v: DataView, o: number): void {
  setI64(v, o, iseq);
  v.setUint32(o + 8, sym, true);
  v.setUint32(o + 12, 0, true);
  let price = 0, qty = 0;
  if (c.kind === "new") {
    v.setUint8(o + 12, 1);
    v.setUint8(o + 13, c.side);
    v.setUint8(o + 14, c.otype);
    v.setUint8(o + 15, c.tif);
    price = c.price;
    qty = c.qty;
  } else if (c.kind === "cancel") {
    v.setUint8(o + 12, 2);
  } else {
    v.setUint8(o + 12, 3);
    price = c.price;
    qty = c.qty;
  }
  setI64(v, o + 16, c.orderId);
  setI64(v, o + 24, price);
  setI64(v, o + 32, qty);
  seal(v, o, CMD_RECORD_V1);
}

export interface CmdRecord {
  iseq: number;
  sym: number;
  cmd: Command;
}

function decodeCmd(v: DataView, o: number): CmdRecord | undefined {
  const iseq = getI64(v, o, false), orderId = getI64(v, o + 16, false), price = getI64(v, o + 24, true), qty = getI64(v, o + 32, false);
  if (iseq === undefined || orderId === undefined || price === undefined || qty === undefined) return undefined;
  const sym = v.getUint32(o + 8, true), side = v.getUint8(o + 13), otype = v.getUint8(o + 14), tif = v.getUint8(o + 15);
  switch (v.getUint8(o + 12)) {
    case 1:
      if (side > 1 || otype > 1 || tif > 3) return undefined;
      return { iseq, sym, cmd: { kind: "new", orderId, side: side as Side, otype: otype as OType, price, qty, tif: tif as Tif } };
    case 2:
      return { iseq, sym, cmd: { kind: "cancel", orderId } };
    case 3:
      return { iseq, sym, cmd: { kind: "replace", orderId, price, qty } };
  }
  return undefined;
}

/// spec/JOURNAL.md §3.2: reject 1..7 in SPEC order, close 1..3.
export function encodeEvt(seq: number, sym: number, e: Event, v: DataView, o: number): void {
  setI64(v, o, seq);
  v.setUint32(o + 8, sym, true);
  let a = 0, b = 0, c = 0, d = 0, ev = 0, reason = 0;
  switch (e.kind) {
    case "accepted": ev = 1; a = e.orderId; d = e.leavesQty; break;
    case "rejected": ev = 2; reason = e.reason + 1; a = e.orderId; break;
    case "trade": ev = 3; a = e.maker; b = e.taker; c = e.price; d = e.qty; break;
    case "closed": ev = 4; reason = e.reason + 1; a = e.orderId; break;
    case "replaced": ev = 5; a = e.orderId; c = e.price; d = e.qty; break;
  }
  v.setUint8(o + 12, ev);
  v.setUint8(o + 13, reason);
  v.setUint16(o + 14, 0, true);
  setI64(v, o + 16, a);
  setI64(v, o + 24, b);
  setI64(v, o + 32, c);
  setI64(v, o + 40, d);
  seal(v, o, EVT_RECORD_V1);
}

function decodeEvt(v: DataView, o: number): string | undefined {
  const seq = getI64(v, o, false), a = getI64(v, o + 16, false), b = getI64(v, o + 24, false);
  const c = getI64(v, o + 32, true), d = getI64(v, o + 40, false);
  if (seq === undefined || a === undefined || b === undefined || c === undefined || d === undefined) return undefined;
  const sym = v.getUint32(o + 8, true), reason = v.getUint8(o + 13);
  let e: Event;
  switch (v.getUint8(o + 12)) {
    case 1: if (reason !== 0) return undefined; e = { kind: "accepted", orderId: a, leavesQty: d }; break;
    case 2: if (reason < 1 || reason > 7) return undefined; e = { kind: "rejected", orderId: a, reason: (reason - 1) as RejectReason }; break;
    case 3: if (reason !== 0) return undefined; e = { kind: "trade", maker: a, taker: b, price: c, qty: d }; break;
    case 4: if (reason < 1 || reason > 3) return undefined; e = { kind: "closed", orderId: a, reason: (reason - 1) as CloseReason }; break;
    case 5: if (reason !== 0) return undefined; e = { kind: "replaced", orderId: a, price: c, qty: d }; break;
    default: return undefined;
  }
  return eventCanonical(seq, e, sym);
}

// ---- reading (recovery) ------------------------------------------------------------------------

/// `version` is the binary journal version (JSONL reports 2); not part of sameHeader.
export interface JournalHeader {
  kind: JournalKind;
  partition: number;
  partitions: number;
  book: BookConfig;
  version: number;
}

function parseJsonlHeader(p: string, line: string): JournalHeader {
  if (get(line, "format") !== "orderer-journal/1") throw corrupt(p, "not an orderer-journal/1 header");
  const k = get(line, "kind");
  if (k !== "cmd" && k !== "evt") throw corrupt(p, "bad kind");
  const part = u64(line, "partition"), parts = u64(line, "partitions");
  if (part === undefined || part > 0xffffffff) throw corrupt(p, "bad partition");
  if (parts === undefined || parts > 0xffffffff) throw corrupt(p, "bad partitions");
  const pmin = i64(line, "pmin"), pmax = i64(line, "pmax"), mo = u64(line, "max_orders"), ix = get(line, "index");
  if (pmin === undefined) throw corrupt(p, "bad pmin");
  if (pmax === undefined) throw corrupt(p, "bad pmax");
  if (mo === undefined) throw corrupt(p, "bad max_orders");
  if (ix !== "ladder" && ix !== "tree") throw corrupt(p, "bad index");
  return { kind: k, partition: part, partitions: parts, version: VERSION,
    book: { priceMin: pmin, priceMax: pmax, maxOrders: mo, index: ix === "tree" ? IndexKind.Tree : IndexKind.Ladder } };
}

function parseBinaryHeader(p: string, b: Buffer): JournalHeader {
  if (b.length < HEADER || b.toString("latin1", 0, 4) !== "ORDJ") throw corrupt(p, "bad magic");
  const v = new DataView(b.buffer, b.byteOffset, b.length);
  const version = v.getUint16(4, true);
  if (version !== 1 && version !== 2) throw corrupt(p, "unsupported version");
  const kind: JournalKind | undefined = b[6] === 1 ? "cmd" : b[6] === 2 ? "evt" : undefined;
  if (kind === undefined) throw corrupt(p, "bad kind");
  if (v.getUint32(16, true) !== recordSize(kind, version)) throw corrupt(p, "bad record_size");
  if (b[7] > 1) throw corrupt(p, "bad index");
  const pmin = getI64(v, 24, true), pmax = getI64(v, 32, true), mo = getI64(v, 40, false);
  if (pmin === undefined || pmax === undefined || mo === undefined) throw corrupt(p, "header value beyond 2^53");
  return { kind, partition: v.getUint32(8, true), partitions: v.getUint32(12, true), version,
    book: { priceMin: pmin, priceMax: pmax, maxOrders: mo, index: b[7] === 1 ? IndexKind.Tree : IndexKind.Ladder } };
}

function readFile(p: string): Buffer {
  try {
    return fs.readFileSync(p);
  } catch (e) {
    throw corrupt(p, (e as Error).message);
  }
}

export function readHeader(p: string, f: JournalFormat): JournalHeader {
  const b = readFile(p);
  if (f === "binary") return parseBinaryHeader(p, b);
  const t = b.toString("utf8");
  const nl = t.indexOf("\n");
  return parseJsonlHeader(p, nl < 0 ? t : t.slice(0, nl));
}

/// Strict (default) or repair reading (spec/JOURNAL.md §5, §5.1).
export type ReadMode = "strict" | "repair";

interface Body {
  header: JournalHeader;
  records: Array<[number, number]>; // offset, length (binary: already checksum-checked)
  validLen: number; // bytes a repair keeps
}

function allZero(b: Buffer, from: number, to: number): boolean {
  for (let i = from; i < to; i++) if (b[i] !== 0) return false;
  return true;
}

function splitBody(p: string, b: Buffer, f: JournalFormat, mode: ReadMode): Body {
  if (f === "binary") {
    const header = parseBinaryHeader(p, b);
    const size = recordSize(header.kind, header.version), body = b.length - HEADER;
    let n = Math.floor(body / size);
    if (body % size !== 0 && mode === "strict") throw corrupt(p, "torn tail (partial record)");
    const v = new DataView(b.buffer, b.byteOffset, b.length);
    if (mode === "repair") {
      // 1.3: zero records an interrupted write left (§5.1)
      while (n > 0 && allZero(b, HEADER + (n - 1) * size, HEADER + n * size)) n--;
    }
    if (header.version >= 2) {
      for (let i = 0; i < n; i++) {
        if (!sealed(v, HEADER + i * size, payloadOf(header.kind))) {
          if (mode === "repair" && i + 1 === n) { n--; break; } // a torn final record (§5.1)
          throw corrupt(p, `record ${i}: checksum mismatch`);
        }
      }
    }
    const records: Array<[number, number]> = [];
    for (let i = 0; i < n; i++) records.push([HEADER + i * size, size]);
    return { header, records, validLen: HEADER + n * size };
  }
  let end = b.length;
  if (end > 0 && b[end - 1] !== 10) {
    if (mode === "strict") throw corrupt(p, "torn tail (final line has no newline)");
    end = b.lastIndexOf(10) + 1;
  }
  let first = b.indexOf(10);
  if (first < 0 || first > end) first = end;
  const header = parseJsonlHeader(p, b.toString("utf8", 0, first));
  const records: Array<[number, number]> = [];
  for (let pos = first + 1; pos < end; ) {
    const e = b.indexOf(10, pos);
    records.push([pos, e - pos]);
    pos = e + 1;
  }
  return { header, records, validLen: end };
}

function decodeCmds(p: string, b: Buffer, f: JournalFormat, body: Body): CmdRecord[] {
  const v = new DataView(b.buffer, b.byteOffset, b.length);
  return body.records.map(([off, len], i) => {
    if (f === "binary") {
      const r = decodeCmd(v, off);
      if (r === undefined) throw corrupt(p, `record ${i}: bad codes`);
      return r;
    }
    const l = b.toString("utf8", off, off + len);
    const iseq = u64(l, "iseq"), sym = u64(l, "symbol"), cmd = parseCommand(l);
    if (iseq === undefined || sym === undefined || sym > 0xffffffff || cmd === undefined)
      throw corrupt(p, `line ${i + 2}: malformed record: ${l}`);
    return { iseq, sym, cmd };
  });
}

function checkIncreasing(p: string, recs: CmdRecord[], after?: number): void {
  for (const r of recs) {
    if (after !== undefined && r.iseq <= after) throw corrupt(p, `iseq not increasing (${after} then ${r.iseq})`);
    after = r.iseq;
  }
}

/// One command journal file, strictly.
export function readCmdJournal(p: string, f: JournalFormat): { header: JournalHeader; records: CmdRecord[] } {
  const b = readFile(p);
  const body = splitBody(p, b, f, "strict");
  if (body.header.kind !== "cmd") throw corrupt(p, "not a command journal");
  const records = decodeCmds(p, b, f, body);
  checkIncreasing(p, records);
  return { header: body.header, records };
}

/// Every partition's command journal in `dir`, all segments in order (spec/JOURNAL.md §1, §5).
export function readCmdDir(dir: string, f: JournalFormat): { header: JournalHeader; partitions: CmdRecord[][] } {
  const segs = listSegments(dir, "cmd", f);
  if (segs.length === 0) throw corrupt(journalPath(dir, "cmd", 0, f), "no command journal");
  const h0 = readHeader(segs[0].path, f);
  const partitions: CmdRecord[][] = Array.from({ length: h0.partitions }, () => []);
  const seen = new Array<boolean>(h0.partitions).fill(false);
  for (const s of segs) {
    const c = readCmdJournal(s.path, f);
    const h = c.header;
    if (h.partition !== s.partition || s.partition >= h0.partitions || h.partitions !== h0.partitions || !sameBook(h.book, h0.book))
      throw corrupt(s.path, "header does not match its file name, partition count or book config");
    const part = partitions[s.partition];
    checkIncreasing(s.path, c.records, part.length > 0 ? part[part.length - 1].iseq : undefined);
    for (const r of c.records) part.push(r);
    seen[s.partition] = true;
  }
  const missing = seen.indexOf(false);
  if (missing >= 0) throw corrupt(journalPath(dir, "cmd", missing, f), "partition has no journal");
  return { header: h0, partitions };
}

/// An event journal file as canonical symbol-tagged lines.
export function readEvtJournal(p: string, f: JournalFormat): string[] {
  const b = readFile(p);
  const body = splitBody(p, b, f, "strict");
  const v = new DataView(b.buffer, b.byteOffset, b.length);
  return body.records.map(([off, len], i) => {
    if (f === "jsonl") return b.toString("utf8", off, off + len);
    const l = decodeEvt(v, off);
    if (l === undefined) throw corrupt(p, `record ${i}: bad codes`);
    return l;
  });
}

/// A partition's whole event journal (all segments, in order).
export function readEvtPartition(dir: string, f: JournalFormat, p: number): string[] {
  return listSegments(dir, "evt", f).filter((s) => s.partition === p).flatMap((s) => readEvtJournal(s.path, f));
}

/// spec/JOURNAL.md §5.1: truncate a torn tail off each journal family's last segment, in place.
export function repairDir(dir: string, f: JournalFormat): Array<{ path: string; bytes: number }> {
  const out: Array<{ path: string; bytes: number }> = [];
  for (const k of ["cmd", "evt"] as JournalKind[]) {
    const parts = new Map<number, Segment[]>();
    for (const s of listSegments(dir, k, f)) {
      const l = parts.get(s.partition);
      if (l) l.push(s);
      else parts.set(s.partition, [s]);
    }
    for (const segs of parts.values()) {
      segs.sort((a, b) => a.start - b.start);
      // 1.3: drop trailing segments a crash left without a usable header;
      // the segment before becomes the last
      while (segs.length > 0 && segs[segs.length - 1].start > 0) {
        const p = segs[segs.length - 1].path;
        const b = readFile(p);
        if (!headerless(p, b, f)) break;
        fs.unlinkSync(p);
        try {
          const d = fs.openSync(dir, "r");
          try {
            fs.fsyncSync(d);
          } finally {
            fs.closeSync(d);
          }
        } catch {
          // some platforms cannot sync a directory
        }
        out.push({ path: p, bytes: b.length });
        segs.pop();
      }
      // repair the last segment; while it holds no records, the one before
      // it too (its writer may still have been finishing it)
      for (let i = segs.length - 1; i >= 0; i--) {
        const p = segs[i].path;
        const b = readFile(p);
        const body = splitBody(p, b, f, "repair");
        if (body.validLen < b.length) {
          const fd = fs.openSync(p, "r+");
          try {
            fs.ftruncateSync(fd, body.validLen);
            fs.fsyncSync(fd);
          } finally {
            fs.closeSync(fd);
          }
          out.push({ path: p, bytes: b.length - body.validLen });
        }
        if (body.records.length > 0) break;
      }
    }
  }
  return out;
}

/// A segment that cannot hold a record (spec/JOURNAL.md 1.3 §5.1): JSONL with
/// no newline at all, or binary with an invalid header and nothing but zeros
/// after it.
function headerless(p: string, b: Buffer, f: JournalFormat): boolean {
  if (f === "jsonl") return b.indexOf(10) < 0;
  if (!allZero(b, Math.min(HEADER, b.length), b.length)) return false;
  try {
    parseBinaryHeader(p, b);
    return false;
  } catch (e) {
    if (e instanceof CorruptJournal) return true;
    throw e;
  }
}

/// One iseq-ordered stream of records after `after`; iseqs must be disjoint.
export function mergeJournals(parts: CmdRecord[][], after: number): CmdRecord[] {
  const all = parts.flat().filter((r) => r.iseq > after);
  all.sort((a, b) => a.iseq - b.iseq);
  for (let i = 1; i < all.length; i++)
    if (all[i].iseq === all[i - 1].iseq) throw new CorruptJournal(`iseq ${all[i].iseq} appears in two partitions`);
  return all;
}

// ---- writing -----------------------------------------------------------------------------------

/// Create segment `start` (truncating any old file) with its header; returns its path.
export function openSegment(dir: string, f: JournalFormat, k: JournalKind, p: number, P: number, book: BookConfig, start: number): string {
  const pth = segmentPath(dir, k, p, start, f);
  fs.writeFileSync(pth, f === "jsonl" ? Buffer.from(jsonlHeader(k, p, P, book)) : binaryHeader(k, p, P, book));
  return pth;
}

/// Append mode: the partition's last segment (header checked); otherwise a
/// fresh segment 0. Returns the path the journal's I/O worker opens for appending.
export function openJournal(cfg: JournalConfig, k: JournalKind, p: number, P: number, book: BookConfig): string {
  if (cfg.append) {
    const segs = listSegments(cfg.dir, k, cfg.format).filter((s) => s.partition === p);
    if (segs.length > 0) {
      const last = segs[segs.length - 1].path;
      const h = readHeader(last, cfg.format);
      if (h.kind !== k || h.partition !== p || h.partitions !== P || !sameBook(h.book, book))
        throw corrupt(last, "header does not match the pipeline");
      if (cfg.format === "binary" && h.version !== VERSION) throw corrupt(last, "cannot append to a version-1 journal");
      fs.accessSync(last, fs.constants.W_OK);
      return last;
    }
  }
  return openSegment(cfg.dir, cfg.format, k, p, P, book, 0);
}

/// Remove checkpoints with cut below `n` (body first).
export function removeCheckpointsBelow(dir: string, n: number): void {
  for (const c of listCheckpoints(dir, false, n)) {
    fs.rmSync(c.path, { force: true });
    fs.rmSync(c.path + ".meta", { force: true });
  }
}

/// spec/JOURNAL.md §6 step 4: remove segments that start below `n`.
export function removeSegmentsBelow(dir: string, f: JournalFormat, n: number): void {
  for (const k of ["cmd", "evt"] as JournalKind[])
    for (const s of listSegments(dir, k, f)) if (s.start < n) fs.rmSync(s.path, { force: true });
}

/// A fresh (non-append) pipeline owns its directory's journals.
export function clearJournalDir(dir: string, f: JournalFormat): void {
  removeSegmentsBelow(dir, f, Infinity);
  removeCheckpointsBelow(dir, Infinity);
}

/// Write `contents` durably: temporary name, sync, rename, sync the directory.
export function writeDurably(p: string, contents: string): void {
  const tmp = p + ".tmp";
  const fd = fs.openSync(tmp, "w");
  try {
    fs.writeSync(fd, contents);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(tmp, p);
  try {
    const d = fs.openSync(path.dirname(p), "r");
    try { fs.fsyncSync(d); } finally { fs.closeSync(d); }
  } catch {
    /* some platforms cannot sync a directory */
  }
}

// ---- the chunk writer ----------------------------------------------------------------------------
//
// Shared memory: control words, two index queues (owner → I/O, I/O → owner),
// per-chunk metadata, an error string, then the chunks. The owner encodes
// records into its current chunk and hands full or idle chunks to the I/O
// worker, which writes everything queued, then decides on one fsync (group
// commit; libuv uses F_FULLFSYNC on macOS), then publishes the flushed and
// durable watermarks.

const CHUNK = 1 << 18, CHUNKS = 64, Q = CHUNKS + 2;
const TO_HEAD = 0, TO_TAIL = 1, FREE_HEAD = 2, FREE_TAIL = 3, DONE = 4, ERR_LEN = 5, CTRL = 8;
const TOQ = CTRL, FREEQ = CTRL + Q, CTRL_WORDS = CTRL + 2 * Q;
const META_OFF = CTRL_WORDS * 4; // float64 × 3 per chunk: len, last id, records
const ERR_OFF = META_OFF + CHUNKS * 24, ERR_BYTES = 1024;
// rotation slots: the next segment's path, handed to the I/O worker (§6 step 2)
const ROT_SLOTS = 8, ROT_BYTES = 1024, ROT_OFF = ERR_OFF + ERR_BYTES;
const DATA_OFF = Math.ceil((ROT_OFF + ROT_SLOTS * (ROT_BYTES + 4)) / 64) * 64;
const STOP = -1;
const ROTATE = -2; // -2 - slot

/// Everything the I/O worker needs.
export interface ChunkShared {
  sab: SharedArrayBuffer;
  path: string; // opened for appending by the I/O worker
  fsync: FsyncPolicy | null; // null: never sync, durable = flushed
  marks: SharedArrayBuffer; // BigInt64Array [flushed, durable, fsyncs, fsync ns total, fsync ns max]
}

export function createChunkShared(path: string, fsync: FsyncPolicy | null, marks: SharedArrayBuffer): ChunkShared {
  const sab = new SharedArrayBuffer(DATA_OFF + CHUNKS * CHUNK);
  const ctl = new Int32Array(sab, 0, CTRL_WORDS);
  for (let i = 1; i < CHUNKS; i++) ctl[FREEQ + (i - 1)] = i;
  ctl[FREE_TAIL] = CHUNKS - 1;
  return { sab, path, fsync, marks };
}

/// The owner's side: encode into the current chunk, hand chunks off.
export class ChunkWriter {
  private readonly ctl: Int32Array;
  private readonly meta: Float64Array;
  readonly view: DataView;
  private readonly bytes: Uint8Array;
  private cur = 0;
  private len = 0;
  private last = 0;
  private records = 0;
  private finished = false;
  constructor(readonly shared: ChunkShared, private readonly format: JournalFormat) {
    this.ctl = new Int32Array(shared.sab, 0, CTRL_WORDS);
    this.meta = new Float64Array(shared.sab, META_OFF, CHUNKS * 3);
    this.view = new DataView(shared.sab);
    this.bytes = new Uint8Array(shared.sab);
  }
  pending(): number {
    return this.len;
  }
  private reserve(n: number): number {
    if (this.len + n > CHUNK) this.handOff();
    return DATA_OFF + this.cur * CHUNK + this.len;
  }
  private putAscii(o: number, s: string): number {
    for (let i = 0; i < s.length; i++) this.bytes[o + i] = s.charCodeAt(i);
    return s.length;
  }
  pushCmd(iseq: number, sym: number, c: Command): void {
    const o = this.reserve(MAX_RECORD);
    if (this.format === "binary") {
      encodeCmd(iseq, sym, c, this.view, o);
      this.len += CMD_RECORD;
    } else {
      this.len += this.putAscii(o, cmdLine(iseq, sym, c) + "\n");
    }
    this.last = iseq;
    this.records++;
  }
  pushEvt(seq: number, sym: number, e: Event): void {
    const o = this.reserve(MAX_RECORD);
    if (this.format === "binary") {
      encodeEvt(seq, sym, e, this.view, o);
      this.len += EVT_RECORD;
    } else {
      this.len += this.putAscii(o, eventCanonical(seq, e, sym) + "\n");
    }
    this.last = seq;
    this.records++;
  }
  /// Hand the chunk to the I/O worker (blocks only when every chunk is in flight).
  handOff(): void {
    if (this.len === 0) return;
    this.meta[this.cur * 3] = this.len;
    this.meta[this.cur * 3 + 1] = this.last;
    this.meta[this.cur * 3 + 2] = this.records;
    this.records = 0;
    this.enqueue(this.cur);
    const ctl = this.ctl;
    for (;;) {
      const head = Atomics.load(ctl, FREE_HEAD), tail = Atomics.load(ctl, FREE_TAIL);
      if (head !== tail) {
        this.cur = ctl[FREEQ + (head % Q)];
        Atomics.store(ctl, FREE_HEAD, head + 1);
        break;
      }
      Atomics.wait(ctl, FREE_TAIL, tail, 1);
    }
    this.len = 0;
  }
  private enqueue(c: number): void {
    const tail = Atomics.load(this.ctl, TO_TAIL);
    this.ctl[TOQ + (tail % Q)] = c;
    Atomics.store(this.ctl, TO_TAIL, tail + 1);
    Atomics.notify(this.ctl, TO_TAIL);
  }
  private rotations = 0;
  /// Continue in the segment at `next` (header written): everything so far
  /// goes to the current file, which the I/O worker syncs per policy and closes.
  rotate(next: string): void {
    this.handOff();
    const slot = this.rotations++ % ROT_SLOTS;
    const b = Buffer.from(next, "utf8");
    if (b.length > ROT_BYTES) throw new Error(`segment path too long: ${next}`);
    const v = new DataView(this.shared.sab);
    v.setUint32(ROT_OFF + slot * (ROT_BYTES + 4), b.length, true);
    new Uint8Array(this.shared.sab, ROT_OFF + slot * (ROT_BYTES + 4) + 4, b.length).set(b);
    this.enqueue(ROTATE - slot);
  }
  /// Write and (per policy) sync everything; waits for the I/O worker. The first I/O error, if any.
  finish(): string | undefined {
    if (!this.finished) {
      this.finished = true;
      this.handOff();
      this.enqueue(STOP);
      while (Atomics.load(this.ctl, DONE) === 0) Atomics.wait(this.ctl, DONE, 0, 10);
    }
    return this.error();
  }
  isDone(): boolean {
    return Atomics.load(this.ctl, DONE) !== 0;
  }
  error(): string | undefined {
    const n = Atomics.load(this.ctl, ERR_LEN);
    return n === 0 ? undefined : Buffer.from(new Uint8Array(this.shared.sab, ERR_OFF, n)).toString("utf8");
  }
}

/// The I/O worker's loop: runs until the owner sends STOP, or until the
/// pipeline is alerted (a failure) and nothing is queued.
export function ioLoop(sh: ChunkShared, alert?: SharedArrayBuffer): void {
  const ctl = new Int32Array(sh.sab, 0, CTRL_WORDS);
  const alerted = alert === undefined ? undefined : new Int32Array(alert);
  let fd = -1;
  const meta = new Float64Array(sh.sab, META_OFF, CHUNKS * 3);
  const marks = new BigInt64Array(sh.marks);
  const setErr = (m: string) => {
    if (Atomics.load(ctl, ERR_LEN) !== 0) return;
    const b = Buffer.from(m.slice(0, 1000), "utf8");
    new Uint8Array(sh.sab, ERR_OFF, b.length).set(b);
    Atomics.store(ctl, ERR_LEN, b.length);
  };
  try {
    fd = fs.openSync(sh.path, "a");
  } catch (e) {
    setErr(`journal open: ${(e as Error).message}`);
  }
  let unsynced = 0, written = Number(Atomics.load(marks, 0)), lastSync = performance.now();
  const sync = () => {
    if (Atomics.load(ctl, ERR_LEN) === 0) {
      try {
        const t0 = performance.now();
        fs.fsyncSync(fd);
        const ns = BigInt(Math.round((performance.now() - t0) * 1e6));
        if (marks.length >= 5) { // [flushed, durable, fsyncs, fsync ns total, fsync ns max]
          Atomics.add(marks, 2, 1n);
          Atomics.add(marks, 3, ns);
          if (ns > Atomics.load(marks, 4)) Atomics.store(marks, 4, ns);
        }
        Atomics.store(marks, 1, BigInt(written));
      } catch (e) {
        setErr(`journal fsync: ${(e as Error).message}`);
      }
    }
    unsynced = 0;
    lastSync = performance.now();
  };
  const idleMs = sh.fsync === null ? Infinity : sh.fsync.mode === "everyN" ? sh.fsync.idleUs / 1000
    : sh.fsync.mode === "every" ? sh.fsync.intervalUs / 1000 : Infinity;
  for (;;) {
    let head = Atomics.load(ctl, TO_HEAD);
    if (head === Atomics.load(ctl, TO_TAIL)) {
      if (unsynced > 0) {
        if (Atomics.wait(ctl, TO_TAIL, head, idleMs) === "timed-out" && head === Atomics.load(ctl, TO_TAIL)) sync();
      } else {
        Atomics.wait(ctl, TO_TAIL, head, 20);
        if (alerted !== undefined && Atomics.load(alerted, 0) !== 0 && head === Atomics.load(ctl, TO_TAIL)) {
          try { fs.closeSync(fd); } catch { /* failing anyway */ }
          Atomics.store(ctl, DONE, 1);
          Atomics.notify(ctl, DONE);
          return;
        }
      }
      continue;
    }
    let stop = false;
    while (head !== Atomics.load(ctl, TO_TAIL)) { // write everything queued, then one fsync decision
      const c = ctl[TOQ + (head % Q)];
      Atomics.store(ctl, TO_HEAD, ++head);
      if (c === STOP) {
        stop = true;
        break;
      }
      if (c <= ROTATE) {
        const slot = ROTATE - c, at = ROT_OFF + slot * (ROT_BYTES + 4);
        const len = new DataView(sh.sab).getUint32(at, true);
        const next = Buffer.from(new Uint8Array(sh.sab, at + 4, len)).toString("utf8");
        if (unsynced > 0 && sh.fsync !== null && sh.fsync.mode !== "never") sync();
        unsynced = 0;
        try {
          fs.closeSync(fd);
          fd = fs.openSync(next, "a");
        } catch (e) {
          setErr(`journal rotate: ${(e as Error).message}`);
        }
        continue;
      }
      const len = meta[c * 3];
      try {
        let off = 0;
        while (off < len) off += fs.writeSync(fd, new Uint8Array(sh.sab, DATA_OFF + c * CHUNK + off, len - off));
      } catch (e) {
        setErr(`journal write: ${(e as Error).message}`);
      }
      written = meta[c * 3 + 1];
      Atomics.store(marks, 0, BigInt(written));
      unsynced += Math.max(meta[c * 3 + 2], 1);
      const ft = Atomics.load(ctl, FREE_TAIL);
      ctl[FREEQ + (ft % Q)] = c;
      Atomics.store(ctl, FREE_TAIL, ft + 1);
      Atomics.notify(ctl, FREE_TAIL);
    }
    const f = sh.fsync;
    let due = false;
    if (f === null || f.mode === "never") {
      Atomics.store(marks, 1, BigInt(written));
      unsynced = 0;
    } else if (f.mode === "everyN") due = unsynced >= f.n;
    else due = performance.now() - lastSync >= f.intervalUs / 1000;
    if (due || (stop && unsynced > 0)) sync();
    if (stop) {
      try {
        fs.closeSync(fd);
      } catch (e) {
        setErr(`journal close: ${(e as Error).message}`);
      }
      Atomics.store(ctl, DONE, 1);
      Atomics.notify(ctl, DONE);
      return;
    }
  }
}

