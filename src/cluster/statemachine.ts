// The StateMachine interface the log drives, plus the language-neutral
// RecorderSm and the exactly-once DedupSm wrapper (ports of orderer-rust's
// recorder.rs and dedup.rs).

import { createHash } from "crypto";
import { Entry } from "../consensus";
import { Out, sessionOf } from "./codec";

export type Digest = Buffer;

export const sha256 = (...parts: Uint8Array[]): Digest => {
  const h = createHash("sha256");
  for (const p of parts) h.update(p);
  return h.digest();
};

/// The deterministic state machine the replicated log drives. Called only
/// from the replica, in log order.
export interface StateMachine {
  /// Apply committed entry `index`. No-op entries arrive with empty data.
  /// Returning false means "not finished, call again with the same entry
  /// later" (a state machine whose downstream is full): the replica keeps
  /// serving the network in between, which one blocking apply could not.
  apply(index: number, entry: Entry): void | false;
  /// Serialize the state after applying `index`.
  snapshot(index: number): Uint8Array;
  /// Replace the state with a snapshot taken at `index`; an error string on failure.
  restore(index: number, data: Uint8Array): string | undefined;
  /// A digest of the state, for divergence detection; undefined = unsupported.
  digest(index: number): Digest | undefined;
}

/// Records every non-empty entry. Snapshot = the list `index u64 | len u32 |
/// data`; digest = SHA-256 of that list. Identical to orderer-rust's RecorderSm.
export class RecorderSm implements StateMachine {
  applied: Array<{ index: number; data: Uint8Array }> = [];
  apply(index: number, e: Entry): void {
    if (e.data.length > 0) this.applied.push({ index, data: e.data });
  }
  private serialize(): Buffer {
    const o = new Out(1024);
    for (const a of this.applied) o.u64(a.index).u32(a.data.length).bytes(a.data);
    return o.done();
  }
  snapshot(): Uint8Array { return this.serialize(); }
  restore(_index: number, d: Uint8Array): string | undefined {
    const b = Buffer.from(d.buffer, d.byteOffset, d.length);
    const v: Array<{ index: number; data: Uint8Array }> = [];
    for (let at = 0; at < b.length; ) {
      if (b.length - at < 12) return "short recorder snapshot";
      const index = b.readUInt32LE(at) + b.readUInt32LE(at + 4) * 4294967296;
      const len = b.readUInt32LE(at + 8);
      if (at + 12 + len > b.length) return "short recorder snapshot";
      v.push({ index, data: new Uint8Array(b.subarray(at + 12, at + 12 + len)) });
      at += 12 + len;
    }
    this.applied = v;
    return undefined;
  }
  digest(): Digest { return sha256(this.serialize()); }
}

/// Applies a session's batch only when `seq` is newer than the last applied
/// for that client, so a retry that landed in the log twice takes effect once.
/// The table is part of every snapshot and digest.
export class DedupSm implements StateMachine {
  private last = new Map<bigint, bigint>();
  duplicates = 0;
  constructor(private readonly inner: StateMachine) {}

  apply(index: number, e: Entry): void {
    const s = sessionOf(e.data);
    if (s) {
      if (s.seq <= (this.last.get(s.client) ?? 0n)) { this.duplicates++; return; }
      this.last.set(s.client, s.seq);
    }
    this.inner.apply(index, e);
  }

  private table(): Buffer {
    const keys = [...this.last.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const o = new Out(4 + keys.length * 16).u32(keys.length);
    for (const k of keys) o.u64big(k).u64big(this.last.get(k)!);
    return o.done();
  }

  snapshot(index: number): Uint8Array {
    const inner = this.inner.snapshot(index);
    return new Out(1024).bytes(this.table()).u64(inner.length).bytes(inner).done();
  }

  restore(index: number, d: Uint8Array): string | undefined {
    const b = Buffer.from(d.buffer, d.byteOffset, d.length);
    if (b.length < 4) return "short dedup snapshot";
    const count = b.readUInt32LE(0);
    const tableEnd = 4 + count * 16;
    if (b.length < tableEnd + 8) return "short dedup snapshot";
    const last = new Map<bigint, bigint>();
    for (let i = 0; i < count; i++) last.set(b.readBigUInt64LE(4 + i * 16), b.readBigUInt64LE(12 + i * 16));
    const len = b.readUInt32LE(tableEnd) + b.readUInt32LE(tableEnd + 4) * 4294967296;
    if (tableEnd + 8 + len > b.length) return "short dedup snapshot";
    const err = this.inner.restore(index, b.subarray(tableEnd + 8, tableEnd + 8 + len));
    if (err !== undefined) return err;
    this.last = last;
    return undefined;
  }

  digest(index: number): Digest | undefined {
    const inner = this.inner.digest(index);
    return inner === undefined ? undefined : sha256(this.table(), inner);
  }
}
