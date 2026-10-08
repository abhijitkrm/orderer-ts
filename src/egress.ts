//! Pluggable per-partition event consumers (spec/PIPELINE.md §7). Plugs run
//! on the thread that owns the Pipeline: it pumps the outboxes whenever it
//! publishes into a full ring, drains, snapshots or shuts down, and on an
//! unref'd timer otherwise.

import { eventCanonical } from "./matcher/types";
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
