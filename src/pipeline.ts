//! Rings, workers and control (spec/PIPELINE.md).
//!
//!   publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress plugs
//!   (owner     (SAB ring) (worker)│              (worker:       (owner thread,
//!    thread)                      └─▶ …           journal +      pumped)
//!                                                 apply)
//!
//! The thread that builds the Pipeline owns it: it publishes, and it runs
//! the egress plugs by pumping the outboxes — whenever it waits on a full
//! ingress, drains, snapshots or shuts down, and on an unref'd timer
//! otherwise. Router, engines and every journal's I/O run on workers.

import * as fs from "fs";
import * as path from "path";
import { MessageChannel, MessagePort, Worker, receiveMessageOnPort } from "worker_threads";
import { BookConfig, defaultConfig } from "./matcher/book";
import { Command } from "./matcher/types";
import { Block, snapshotHeader } from "./core";
import { EgressCtx, Egress, EgressFactory } from "./egress";
import { ChunkShared, ChunkWriter, JournalConfig, createChunkShared, openJournal } from "./journal";
import { CMD_SLOT, Control, EVT_SLOT, EvtView, evtArg, evtCtl, writeCmd, writeCtl } from "./msg";
import { PartitionMap } from "./routing";
import { Consumer, Publish, RingShared, SingleProducer, WaitStrategy, backoff, busySpin, createRing, parkUs } from "./ring";
import { EngineData, EngineInitial, FAIL_BYTES, IoData, RouterData, readFailure, recordFailure } from "./worker";

export type ErrorKind = "closed" | "full" | "failed" | "config" | "io";

export class PipelineError extends Error {
  constructor(readonly kind: ErrorKind, message: string) {
    super(message);
  }
}

/// Publish outcome: Ok means sequenced and will be applied — not durable.
export enum Status {
  Ok,
  Closed,
  Full,
}

/// Wait strategy per stage (not observable).
export interface Waits {
  router: WaitStrategy;
  engine: WaitStrategy;
  egress: WaitStrategy;
}
export const relaxedWaits = (): Waits => ({ router: backoff(), engine: backoff(), egress: backoff() });
/// Router and engines busy-spin; egress backs off. The bench configuration.
export const lowLatencyWaits = (): Waits => ({ router: busySpin(), engine: busySpin(), egress: backoff() });

/// Starting state after recovery: engines rebuild their own books (cores
/// live in workers) from the snapshot and/or journals; nextIseq continues
/// the sequence (Recovery.initial() fills all three).
export interface Initial extends EngineInitial {
  nextIseq: number;
}

/// A merged matcher-snap/1 snapshot plus its cut (spec/JOURNAL.md §4).
export class Snapshot {
  constructor(readonly body: string, readonly iseq: number, readonly partitions: number) {}
  meta(): string {
    return `{"format":"orderer-meta/1","iseq":${this.iseq},"partitions":${this.partitions}}\n`;
  }
  write(p: string): void {
    fs.writeFileSync(p, this.body);
    fs.writeFileSync(metaPath(p), this.meta());
  }
}

export const metaPath = (p: string) => p + ".meta";

export class Builder {
  private book: BookConfig = defaultConfig();
  private map?: PartitionMap;
  private parts = 1;
  private rings = { ingress: 1 << 14, inbox: 1 << 12, outbox: 1 << 13 }; // tuned in orderer-rust phase 6
  private waitsCfg: Waits = relaxedWaits();
  private journalCfg?: JournalConfig;
  private readonly egressFactories: EgressFactory[] = [];
  private stamps = false;
  private init?: Initial;
  private coreSpec = "fifo";

  /// "fifo" (matcher-ts OrderBook per symbol), "noop", or "module#export" of a CoreFactory.
  core(spec: string): this { this.coreSpec = spec; return this; }
  bookConfig(c: BookConfig): this { this.book = c; return this; }
  partitions(p: number): this { this.parts = p; this.map = undefined; return this; }
  partitionMap(m: PartitionMap): this { this.parts = m.partitions; this.map = m; return this; }
  ringSizes(ingress: number, inbox: number, outbox: number): this { this.rings = { ingress, inbox, outbox }; return this; }
  waits(w: Waits): this { this.waitsCfg = w; return this; }
  journal(j: JournalConfig): this { this.journalCfg = j; return this; }
  egress(f: EgressFactory): this { this.egressFactories.push(f); return this; }
  timestamps(on: boolean): this { this.stamps = on; return this; }
  initial(i: Initial): this { this.init = i; return this; }
  build(): Pipeline {
    return new Pipeline({
      book: this.book, map: this.map, parts: this.parts, rings: this.rings, waits: this.waitsCfg, journal: this.journalCfg,
      egress: this.egressFactories, timestamps: this.stamps, initial: this.init, core: this.coreSpec,
    });
  }
}

interface BuildOpts {
  book: BookConfig;
  map?: PartitionMap;
  parts: number;
  rings: { ingress: number; inbox: number; outbox: number };
  waits: Waits;
  journal?: JournalConfig;
  egress: EgressFactory[];
  timestamps: boolean;
  initial?: Initial;
  core: string;
}

interface EgressPart {
  p: number;
  outbox: Consumer;
  plugs: Egress[];
  evtJournal?: ChunkWriter;
  lastHandoff: number;
  marks?: BigInt64Array;
  lastIseq: number;
  epoch: number;
  stopSeen: boolean;
  stopped: boolean;
  handler: (v: DataView, o: number, seq: number, eob: boolean) => void;
}

const WORKER = path.join(__dirname, "worker.js");
const pow2 = (n: number) => n >= 2 && (n & (n - 1)) === 0;

/// A running pipeline. shutdown() stops it.
export class Pipeline {
  static builder(): Builder {
    return new Builder();
  }

  readonly partitions: number;
  readonly bookConfig: BookConfig;
  private readonly map: PartitionMap;
  private readonly ingress: SingleProducer;
  private readonly parts: EgressPart[] = [];
  private readonly workers: Worker[] = [];
  private readonly ports: MessagePort[] = [];
  private readonly fail = new SharedArrayBuffer(FAIL_BYTES);
  private readonly alert = new SharedArrayBuffer(4);
  private readonly exited: Int32Array;
  private readonly marks: BigInt64Array[] = [];
  private readonly epochMs = performance.now();
  private stamps: boolean;
  private closed = false;
  private shut = false;
  private nextEpoch = 0;
  private nextOp = 0;
  private readonly snaps = new Map<number, { blocks: Block[]; remaining: number; cut: number }>();
  private readonly timer: NodeJS.Timeout;
  private readonly pumpFn = () => this.pump();

  constructor(o: BuildOpts) {
    let map: PartitionMap;
    try {
      map = o.map ?? PartitionMap.make(o.parts);
    } catch (e) {
      throw new PipelineError("config", (e as Error).message);
    }
    this.map = map;
    const P = map.partitions;
    this.partitions = P;
    this.bookConfig = o.book;
    this.stamps = o.timestamps;
    if (!pow2(o.rings.ingress) || !pow2(o.rings.inbox) || !pow2(o.rings.outbox))
      throw new PipelineError("config", "ring sizes must be powers of two >= 2");
    const nextIseq = Math.max(o.initial?.nextIseq ?? 1, 1);
    const journaled = o.journal !== undefined;
    const startWm = BigInt(nextIseq - 1);

    // journals first, so I/O errors surface from build()
    const cmdShared: Array<ChunkShared | null> = [], evtShared: Array<ChunkShared | null> = [];
    if (journaled) {
      const jc = o.journal!;
      try {
        fs.mkdirSync(jc.dir, { recursive: true });
        for (let p = 0; p < P; p++) {
          const m = new SharedArrayBuffer(16);
          const marks = new BigInt64Array(m);
          marks[0] = marks[1] = startWm;
          this.marks.push(marks);
          cmdShared.push(createChunkShared(openJournal(jc, "cmd", p, P, o.book), jc.fsync, m));
          if (jc.events) {
            evtShared.push(createChunkShared(openJournal(jc, "evt", p, P, o.book), null, new SharedArrayBuffer(16)));
          } else {
            evtShared.push(null);
          }
        }
      } catch (e) {
        throw new PipelineError("io", (e as Error).message);
      }
    }
    const io = cmdShared.filter((c) => c !== null).length + evtShared.filter((c) => c !== null).length;
    this.exited = new Int32Array(new SharedArrayBuffer(4 * (1 + P + io)));
    let wi = 0;
    const common = (role: "router" | "engine" | "io", name: string) =>
      ({ role, name, fail: this.fail, alert: this.alert, exited: this.exited.buffer as SharedArrayBuffer, index: wi++ });
    const spawn = (data: object, transfer: MessagePort[] = []) => {
      const w = new Worker(WORKER, { workerData: data, transferList: transfer, stdout: true, stderr: true });
      w.on("error", (e: Error) => recordFailure(this.fail, this.alert, `worker failed: ${e.message}`));
      w.unref();
      this.workers.push(w);
    };

    const inboxes: RingShared[] = [];
    const table = map.table();
    for (let p = 0; p < P; p++) {
      const inbox = createRing(o.rings.inbox, CMD_SLOT, "single", [[]], this.alert);
      const outbox = createRing(o.rings.outbox, EVT_SLOT, "single", [[]], this.alert);
      inboxes.push(inbox);
      const ch = new MessageChannel();
      this.ports.push(ch.port1);
      const ed: EngineData = {
        ...common("engine", "engine"), role: "engine", partition: p, partitions: P, table, inbox, outbox, core: o.core,
        book: o.book, journal: journaled ? { shared: cmdShared[p]!, format: o.journal!.format } : null,
        initial: o.initial === undefined ? null : { snapshot: o.initial.snapshot, journal: o.initial.journal },
        port: ch.port2, wait: o.waits.engine,
      };
      spawn(ed, [ch.port2]);
      if (journaled) spawn({ ...common("io", "journal"), role: "io", shared: cmdShared[p]! } as IoData);
      const evt = evtShared[p] ?? null;
      if (evt !== null) spawn({ ...common("io", "journal"), role: "io", shared: evt } as IoData);

      const marks = journaled ? this.marks[p] : undefined;
      const epoch = this.epochMs;
      const ctx: EgressCtx = {
        partition: p,
        partitions: P,
        durableIseq: () => (marks === undefined ? Infinity : Number(Atomics.load(marks, 1))),
        nowNs: () => Math.round((performance.now() - epoch) * 1e6),
      };
      const part: EgressPart = {
        p, outbox: new Consumer(outbox, 0, o.waits.egress), plugs: [], lastHandoff: performance.now(), marks,
        lastIseq: 0, epoch: 0, stopSeen: false, stopped: false, handler: () => {},
      };
      if (evt !== null) part.evtJournal = new ChunkWriter(evt, o.journal!.format);
      for (const f of o.egress) part.plugs.push(f(ctx));
      const view = new EvtView(part.outbox.view);
      part.handler = (v, off, _seq, eob) => {
        const ctl = evtCtl(v, off);
        if (ctl === Control.None) {
          const m = view.at(off);
          part.lastIseq = m.iseq;
          if (part.evtJournal !== undefined) part.evtJournal.pushEvt(m.seq, m.symbol, m.ev);
          for (const pl of part.plugs) pl.onEvent(m);
        } else if (ctl === Control.Barrier) {
          for (const pl of part.plugs) {
            pl.onBatchEnd?.();
            pl.onIdle?.();
          }
          part.epoch = evtArg(v, off);
        } else if (ctl === Control.Shutdown) {
          part.stopSeen = true;
        }
        if (eob) for (const pl of part.plugs) pl.onBatchEnd?.();
      };
      this.parts.push(part);
    }
    const ingress = createRing(o.rings.ingress, CMD_SLOT, "single", [[]], this.alert);
    const rd: RouterData = {
      ...common("router", "router"), role: "router", ingress, inboxes, partitions: P, table, nextIseq, wait: o.waits.router,
    };
    spawn(rd);
    this.ingress = new SingleProducer(ingress);
    this.timer = setInterval(this.pumpFn, 1);
    this.timer.unref();
  }

  partitionOf(sym: number): number {
    return this.map.partition(sym);
  }
  setTimestamps(on: boolean): void {
    this.stamps = on;
  }
  durableIseq(p: number): number {
    return this.marks.length === 0 ? Infinity : Number(Atomics.load(this.marks[p], 1));
  }

  private now(): number {
    return this.stamps ? Math.max(Math.round((performance.now() - this.epochMs) * 1e6), 1) : 0;
  }

  /// Sequence one command (waits — pumping egress — while ingress is full).
  publish(sym: number, cmd: Command): Status {
    if (this.closed) return Status.Closed;
    const off = this.ingress.stage(this.pumpFn);
    if (off < 0) return Status.Closed;
    writeCmd(this.ingress.view, off, sym, cmd, this.now());
    this.ingress.commit();
    return Status.Ok;
  }

  /// Sequence one command, or Full without waiting.
  tryPublish(sym: number, cmd: Command): Status {
    if (this.closed) return Status.Closed;
    if (!this.ingress.hasRoom(1)) return Status.Full;
    return this.publish(sym, cmd);
  }

  /// Many commands, one claim per chunk (consecutive iseqs within a chunk).
  publishBatch(cmds: ReadonlyArray<readonly [number, Command]>, from = 0, to = cmds.length): Status {
    if (this.closed) return Status.Closed;
    const chunk = Math.min(this.ingress.size, 256);
    for (let off = from; off < to; off += chunk) {
      const k = Math.min(chunk, to - off), t = this.now();
      const r = this.ingress.publishBatch(k, (i, v, o) => writeCmd(v, o, cmds[off + i][0], cmds[off + i][1], t), this.pumpFn);
      if (r !== Publish.Ok) return Status.Closed;
    }
    return Status.Ok;
  }

  private failure(): string | undefined {
    return readFailure(this.fail);
  }

  private check(): void {
    const f = this.failure();
    if (f !== undefined) throw new PipelineError("failed", `pipeline failed: ${f}`);
  }

  /// Run the egress plugs over everything the outboxes hold; events handled.
  pump(): number {
    let total = 0;
    try {
      for (const ep of this.parts) {
        if (ep.stopped) continue;
        const n = ep.outbox.poll(ep.handler);
        total += n;
        if (ep.stopSeen) this.stopPart(ep);
        else if (n === 0) {
          if (ep.evtJournal !== undefined && ep.evtJournal.pending() > 0 && performance.now() - ep.lastHandoff >= 0.05) {
            ep.evtJournal.handOff();
            ep.lastHandoff = performance.now();
          }
          for (const pl of ep.plugs) pl.onIdle?.();
        }
      }
    } catch (e) {
      recordFailure(this.fail, this.alert, `egress failed: ${(e as Error).message}`);
    }
    return total;
  }

  private stopPart(ep: EgressPart): void {
    if (ep.marks !== undefined) {
      const marks = ep.marks;
      this.waitUntil(() => Number(Atomics.load(marks, 1)) >= ep.lastIseq, false);
    }
    if (ep.evtJournal !== undefined) {
      const e = ep.evtJournal.finish();
      if (e !== undefined) throw new Error(`event journal: ${e}`);
    }
    for (const pl of ep.plugs) {
      pl.onIdle?.();
      pl.onShutdown?.();
    }
    ep.stopped = true;
  }

  /// pump → spin → short parks until done, failing fast.
  private waitUntil(done: () => boolean, pumping = true): void {
    for (let step = 0; ; step++) {
      if (pumping && this.pump() > 0) step = 0;
      if (done()) return;
      this.check();
      if (step > 64) parkUs(step < 256 ? 1 : 50);
    }
  }

  private publishCtl(c: Control, arg: number): void {
    this.check();
    if (this.closed) throw new PipelineError("closed", "pipeline closed");
    const off = this.ingress.stage(this.pumpFn);
    if (off < 0) throw new PipelineError("closed", "pipeline closed");
    writeCtl(this.ingress.view, off, c, arg);
    this.ingress.commit();
  }

  /// Barrier: returns once every command published before the call has been
  /// applied and delivered to every egress plug.
  drain(): void {
    const epoch = ++this.nextEpoch;
    this.publishCtl(Control.Barrier, epoch);
    this.waitUntil(() => this.parts.every((p) => p.epoch >= epoch));
  }

  /// A consistent snapshot of every book, cut at this point of the ingress order.
  snapshot(): Snapshot {
    const op = ++this.nextOp;
    this.snaps.set(op, { blocks: [], remaining: this.partitions, cut: 0 });
    this.publishCtl(Control.Snapshot, op);
    const st = this.snaps.get(op)!;
    this.waitUntil(() => {
      for (const port of this.ports) {
        for (let m = receiveMessageOnPort(port); m !== undefined; m = receiveMessageOnPort(port)) {
          const r = m.message as { op: number; cut: number; blocks: Block[] };
          const s = this.snaps.get(r.op);
          if (s === undefined) continue;
          s.blocks.push(...r.blocks);
          s.cut = r.cut;
          s.remaining--;
        }
      }
      return st.remaining === 0;
    });
    this.snaps.delete(op);
    st.blocks.sort((a, b) => a.symbol - b.symbol);
    return new Snapshot(snapshotHeader(this.bookConfig) + st.blocks.map((b) => b.text).join(""), st.cut, this.partitions);
  }

  /// Stop accepting commands, drain everything sequenced, stop every worker.
  /// Idempotent. Throws PipelineError("failed") if any stage failed.
  shutdown(): void {
    if (this.shut) return this.check();
    this.shut = true;
    clearInterval(this.timer);
    if (this.failure() === undefined) {
      try {
        this.publishCtl(Control.Shutdown, 0);
      } catch {
        /* failure reported below */
      }
    }
    this.closed = true;
    try {
      this.waitUntil(() => this.parts.every((p) => p.stopped));
    } catch {
      /* failure reported below */
    }
    if (this.failure() !== undefined) {
      Atomics.store(new Int32Array(this.alert), 0, 1);
      for (const ep of this.parts) if (ep.evtJournal !== undefined && !ep.evtJournal.isDone()) ep.evtJournal.finish();
    }
    for (let i = 0; i < this.exited.length; i++) {
      for (let t = 0; Atomics.load(this.exited, i) === 0 && t < 30_000; t++) Atomics.wait(this.exited, i, 0, 1);
    }
    for (const p of this.ports) p.close();
    for (const w of this.workers) void w.terminate();
    this.check();
  }
}
