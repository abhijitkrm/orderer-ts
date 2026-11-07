//! Pluggable per-partition event consumers (spec/PIPELINE.md §7). Plugs run
//! on the thread that owns the Pipeline: it pumps the outboxes whenever it
//! publishes into a full ring, drains, snapshots or shuts down, and on an
//! unref'd timer otherwise.

import { CloseReason, RejectReason, eventCanonical } from "./matcher/types";
import { EvtMsg, copyEvtMsg } from "./msg";

/// What a partition's egress instances know.
export interface EgressCtx {
  partition: number;
  partitions: number;
  /// Highest iseq covered by a completed fsync (Infinity without journals).
  durableIseq(): number;
  /// ns since the pipeline epoch (the clock tPub uses).
  nowNs(): number;
}

/// One partition's consumer of events.
export interface Egress {
  onEvent(m: EvtMsg): void; // every event, partition order
  onBatchEnd?(): void; // after a ring batch / before a drain completes
  onIdle?(): void; // while idle: release gated work
  onShutdown?(): void; // once, after every event
  onCheckpoint?(cut: number): void; // a checkpoint cut passed (1.2)
}

export type EgressFactory = (ctx: EgressCtx) => Egress;

// ---- Collect: canonical lines per partition (harnesses, tests) ----------------------------

export class CollectHandle {
  bufs: string[][] = [];
  /// Each partition's lines (and clears them).
  take(): string[] {
    const out = this.bufs.map((b) => b.join(""));
    this.bufs = this.bufs.map(() => []);
    return out;
  }
  /// spec/HARNESS.md §3 listing: partition 0's lines, then 1's, …
  listing(): string {
    return this.bufs.map((b) => b.join("")).join("");
  }
}

export function collect(tagged: boolean): [EgressFactory, CollectHandle] {
  const h = new CollectHandle();
  const f: EgressFactory = (ctx) => {
    while (h.bufs.length < ctx.partitions) h.bufs.push([]);
    return {
      onEvent: (m) => {
        h.bufs[ctx.partition].push(eventCanonical(m.seq, m.ev, tagged ? m.symbol : undefined) + "\n");
      },
    };
  };
  return [f, h];
}

// ---- Callback ----------------------------------------------------------------------------------

/// f(partition, msg) for every event (msg is reused: copyEvtMsg to keep it).
export function callback(f: (partition: number, m: EvtMsg) => void): EgressFactory {
  return (ctx) => ({ onEvent: (m) => f(ctx.partition, m) });
}

// ---- Acks: durability-gated delivery -------------------------------------------------------------

/// f(partition, msg) only once the causing command is durable (spec/PIPELINE.md §5).
export function acks(f: (partition: number, m: EvtMsg) => void): EgressFactory {
  return (ctx) => {
    let pending: EvtMsg[] = [];
    let head = 0;
    const release = () => {
      if (head === pending.length) return;
      const d = ctx.durableIseq();
      while (head < pending.length && pending[head].iseq <= d) f(ctx.partition, pending[head++]);
      if (head === pending.length) {
        pending = [];
        head = 0;
      }
    };
    return { onEvent: (m) => void pending.push(copyEvtMsg(m)), onBatchEnd: release, onIdle: release, onShutdown: release };
  };
}

// ---- Metrics: counts + end-to-end latency ---------------------------------------------------------

export interface PartitionMetrics {
  partition: number;
  events: number;
  commands: number;
  trades: number;
  latencies: Float64Array; // ns, arrival order
  samples: number;
}

/// One latency sample per command, at its first event: now - tPub
/// (spec/BENCH.md §2.2 step 5). Samples preallocated per partition.
export function metrics(sampleCapacity: number): [EgressFactory, PartitionMetrics[]] {
  const results: PartitionMetrics[] = [];
  const f: EgressFactory = (ctx) => {
    const m: PartitionMetrics = { partition: ctx.partition, events: 0, commands: 0, trades: 0, latencies: new Float64Array(sampleCapacity), samples: 0 };
    let lastIseq = 0;
    return {
      onEvent: (e) => {
        m.events++;
        if (e.kind === "trade") m.trades++;
        if (e.iseq !== lastIseq) {
          lastIseq = e.iseq;
          m.commands++;
          if (e.tPub !== 0 && m.samples < m.latencies.length) m.latencies[m.samples++] = Math.max(ctx.nowNs() - e.tPub, 0);
        }
      },
      onShutdown: () => void results.push(m),
    };
  };
  return [f, results];
}

// ---- EventDigest: a rolling hash of each partition's event stream ------------------------------
//
// Pinned exactly in spec/REPLICATION.md 13.16, because replicas of different languages compare the
// values: step(h, x) = rotl((h ^ x) * 0x9E3779B97F4A7C15, 31), u64 wrapping, starting at 0.
// JavaScript has no cheap u64, so a chain is a (hi, lo) pair of u32 and the multiply works on
// 16-bit limbs; BigInt would allocate on every step. Ids, prices and quantities are numbers below
// 2^53 in this port; a negative price folds as its two's-complement u64.

const KH = 0x9e3779b9;
const KL = 0x7f4a7c15;
const TWO32 = 4294967296;

/// High 32 bits of the 64-bit product of two u32.
function mulhi32(a: number, b: number): number {
  const al = a & 0xffff, ah = a >>> 16, bl = b & 0xffff, bh = b >>> 16;
  const p0 = al * bl, p1 = ah * bl, p2 = al * bh, p3 = ah * bh;
  const mid = p1 + (p0 >>> 16);
  const mid2 = p2 + (mid & 0xffff);
  return p3 + Math.floor(mid / 65536) + Math.floor(mid2 / 65536);
}

const numLo = (n: number): number => n >>> 0;
const numHi = (n: number): number => Math.floor(n / TWO32) >>> 0;

/// One chain: `(hi, lo)` of a u64.
class Chain {
  constructor(public hi = 0, public lo = 0) {}

  step(xh: number, xl: number): void {
    const h = (this.hi ^ xh) >>> 0, l = (this.lo ^ xl) >>> 0;
    const lo = Math.imul(l, KL) >>> 0;
    const hi = (mulhi32(l, KL) + Math.imul(h, KL) + Math.imul(l, KH)) >>> 0;
    this.hi = ((lo >>> 1) | (hi << 31)) >>> 0; // rotl 31
    this.lo = ((hi >>> 1) | (lo << 31)) >>> 0;
  }

  num(n: number): void { this.step(numHi(n), numLo(n)); }

  /// Fold one event: the causing command's iseq, symbol and kind, the book's seq, then the kind's fields.
  event(m: EvtMsg): void {
    const ev = m.ev;
    let kind: number;
    switch (ev.kind) {
      case "accepted": kind = 1; break;
      case "rejected": kind = 2; break;
      case "trade": kind = 3; break;
      case "closed": kind = 4; break;
      default: kind = 5;
    }
    this.num(m.iseq);
    this.step(m.symbol >>> 24, ((m.symbol << 8) | kind) >>> 0);
    this.num(m.seq);
    switch (ev.kind) {
      case "accepted": this.num(ev.orderId); this.num(ev.leavesQty); break;
      case "rejected": this.num(ev.orderId); this.num(rejectCode(ev.reason)); break;
      case "trade": this.num(ev.maker); this.num(ev.taker); this.num(ev.price); this.num(ev.qty); break;
      case "closed": this.num(ev.orderId); this.num(closeCode(ev.reason)); break;
      default: this.num(ev.orderId); this.num(ev.price); this.num(ev.qty);
    }
  }

  value(): bigint { return (BigInt(this.hi) << 32n) | BigInt(this.lo); }
}

// Explicit codes (spec 13.16), not enum order.
function rejectCode(r: RejectReason): number {
  switch (r) {
    case RejectReason.InvalidQty: return 0;
    case RejectReason.InvalidPrice: return 1;
    case RejectReason.DuplicateOrderId: return 2;
    case RejectReason.UnknownOrderId: return 3;
    case RejectReason.PostOnlyWouldCross: return 4;
    case RejectReason.FokCannotFill: return 5;
    default: return 6; // BookFull
  }
}

function closeCode(r: CloseReason): number {
  return r === CloseReason.Filled ? 0 : r === CloseReason.Cancelled ? 1 : 2;
}

/// The chain after folding `events` into `start` (tests; the plug does the same per event).
export function chainOf(events: Iterable<EvtMsg>, start = 0n): bigint {
  const c = new Chain(Number(start >> 32n), Number(start & 0xffffffffn));
  for (const m of events) c.event(m);
  return c.value();
}

/// Reads the chains of an EventDigest. Values are current as of the last pipeline drain.
export class EventDigestHandle {
  constructor(private readonly chains: Chain[]) {}
  /// Every partition's chain, in partition order.
  values(): bigint[] { return this.chains.map((c) => c.value()); }
}

/// Keeps one rolling hash per partition over the events it emits. Replicas fed the same commands
/// emit the same events, so their chains agree and a divergence in behaviour makes them differ.
/// Reading is O(partitions): no snapshot of the books. It does not see state that has not yet
/// influenced an event (a resting order's price until it trades); a full snapshot comparison does.
/// Cost: decoding each event and about 12 u64 steps in 32-bit pairs per event.
export function eventDigest(partitions: number, initial: bigint[] = []): [EgressFactory, EventDigestHandle] {
  const chains: Chain[] = [];
  for (let i = 0; i < partitions; i++) {
    const v = initial[i] ?? 0n;
    chains.push(new Chain(Number(v >> 32n), Number(v & 0xffffffffn)));
  }
  return [(ctx) => ({ onEvent: (m) => chains[ctx.partition].event(m) }), new EventDigestHandle(chains)];
}

/// The raw step on u64 values (tests check it against BigInt).
export function chainStepBig(h: bigint, x: bigint): bigint {
  const c = new Chain(Number(h >> 32n), Number(h & 0xffffffffn));
  c.step(Number(x >> 32n), Number(x & 0xffffffffn));
  return c.value();
}
