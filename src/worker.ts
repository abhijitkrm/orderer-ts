//! Worker entry point: one role per worker (spec/PIPELINE.md stages).
//!
//!   router:  sole ingress consumer; stamps iseq, routes, broadcasts controls,
//!            commits every inbox once per batch.
//!   engine:  journal-before-apply (encode into its ChunkWriter), then apply,
//!            staging events into its outbox.
//!   io:      one per journal file: write + group-commit fsync.
//!
//! A failing worker records its error in the shared failure block and raises
//! the pipeline-wide alert, so every ring wakes and the main thread reports it.

import { MessagePort, isMainThread, workerData } from "worker_threads";
import { BookConfig } from "./matcher/book";
import { Command, Event } from "./matcher/types";
import { Block, MatchingCore, parseSnapshot, resolveCore } from "./core";
import { ChunkShared, ChunkWriter, JournalFormat, ioLoop, mergeJournals, openSegment, readCmdDir } from "./journal";
import { Control, cmdArg, cmdCtl, cmdIseq, cmdSym, cmdTPub, copyCmd, readCmd, setCmdIseq, writeEvt, writeEvtCtl } from "./msg";
import { PartitionMap } from "./routing";
import { Consumer, RingShared, SingleProducer, WaitStrategy } from "./ring";

/// Shared failure block: [flag, length, …utf8 text].
export const FAIL_BYTES = 4096;

export function recordFailure(fail: SharedArrayBuffer, alert: SharedArrayBuffer, msg: string): void {
  const w = new Int32Array(fail, 0, 2);
  if (Atomics.compareExchange(w, 0, 0, 1) === 0) {
    const b = Buffer.from(msg.slice(0, 2000), "utf8");
    new Uint8Array(fail, 8, b.length).set(b);
    Atomics.store(w, 1, b.length);
  }
  const a = new Int32Array(alert);
  Atomics.store(a, 0, 1);
}

export function readFailure(fail: SharedArrayBuffer): string | undefined {
  const w = new Int32Array(fail, 0, 2);
  if (Atomics.load(w, 0) === 0) return undefined;
  for (let i = 0; i < 1000 && Atomics.load(w, 1) === 0; i++) Atomics.wait(w, 1, 0, 1);
  return Buffer.from(new Uint8Array(fail, 8, Atomics.load(w, 1))).toString("utf8");
}

interface Common {
  role: "router" | "engine" | "io";
  name: string;
  fail: SharedArrayBuffer;
  alert: SharedArrayBuffer;
  exited: SharedArrayBuffer; // Int32 per worker
  index: number;
}

export interface RouterData extends Common {
  role: "router";
  ingress: RingShared;
  inboxes: RingShared[];
  partitions: number;
  table: Array<[number, number]>;
  nextIseq: number;
  wait: WaitStrategy;
}

/// How an engine builds its starting books: empty, or restored from a
/// snapshot body and/or replayed from command journals after the cut.
export interface EngineInitial {
  snapshot?: { body: string; iseq: number };
  journal?: { dir: string; format: JournalFormat };
}

export interface EngineData extends Common {
  role: "engine";
  partition: number;
  partitions: number;
  table: Array<[number, number]>;
  inbox: RingShared;
  outbox: RingShared;
  core: string;
  book: BookConfig;
  journal: { shared: ChunkShared; format: JournalFormat; dir: string } | null;
  initial: EngineInitial | null;
  port: MessagePort; // snapshot replies
  wait: WaitStrategy;
  counters: SharedArrayBuffer; // Float64Array [commands, events], published once per batch
}

export interface IoData extends Common {
  role: "io";
  shared: ChunkShared;
}

function router(d: RouterData): void {
  const ingress = new Consumer(d.ingress, 0, d.wait);
  const inboxes = d.inboxes.map((r) => new SingleProducer(r));
  const map = PartitionMap.make(d.partitions, d.table);
  let iseq = d.nextIseq - 1;
  let stop = false;
  const h = (v: DataView, o: number, _seq: number, eob: boolean) => {
    const ctl = cmdCtl(v, o);
    if (ctl === Control.None) {
      iseq++;
      const ib = inboxes[map.partition(cmdSym(v, o))];
      const s = ib.stage();
      if (s >= 0) {
        copyCmd(v, o, ib.view, s);
        setCmdIseq(ib.view, s, iseq);
      }
    } else {
      for (const ib of inboxes) {
        const s = ib.stage();
        if (s >= 0) {
          copyCmd(v, o, ib.view, s);
          setCmdIseq(ib.view, s, iseq);
        }
      }
      if (ctl === Control.Shutdown) stop = true;
    }
    if (eob) for (const ib of inboxes) ib.commit();
  };
  while (!stop && ingress.waitPoll(h)) {
    /* poll */
  }
  for (const ib of inboxes) ib.commit();
}

/// Restore this partition's books and replay its journal records after the cut.
function buildCore(d: EngineData): MatchingCore {
  const factory = resolveCore(d.core);
  const map = PartitionMap.make(d.partitions, d.table);
  const init = d.initial;
  if (init === null) return factory(d.book);
  let cut = 0;
  let core = factory(d.book);
  if (init.snapshot !== undefined) {
    const ps = parseSnapshot(init.snapshot.body);
    core = factory(ps.cfg);
    for (const b of ps.books) {
      if (map.partition(b.symbol) !== d.partition) continue;
      const err = core.restoreBook(b.symbol, b.seq, b.orders);
      if (err !== undefined) throw new Error(`snapshot: ${err}`);
    }
    cut = init.snapshot.iseq;
  }
  if (init.journal !== undefined) {
    const j = readCmdDir(init.journal.dir, init.journal.format);
    for (const r of mergeJournals(j.partitions, cut))
      if (map.partition(r.sym) === d.partition) core.apply(r.sym, r.cmd, () => {});
  }
  return core;
}

function engine(d: EngineData): void {
  const inbox = new Consumer(d.inbox, 0, d.wait);
  const out = new SingleProducer(d.outbox);
  const core = buildCore(d);
  const journal = d.journal === null ? null : new ChunkWriter(d.journal.shared, d.journal.format);
  let iseq = 0, tPub = 0, stop = false, force = false, nCommands = 0, nEvents = 0;
  const counters = new Float64Array(d.counters);
  const emit = (sym: number, seq: number, ev: Event) => {
    nEvents++;
    const s = out.stage();
    if (s < 0) throw new Error("outbox alerted");
    writeEvt(out.view, s, iseq, seq, tPub, sym, ev);
  };
  const h = (v: DataView, o: number, _seq: number, eob: boolean) => {
    const ctl = cmdCtl(v, o);
    if (ctl === Control.None) {
      iseq = cmdIseq(v, o);
      tPub = cmdTPub(v, o);
      const sym = cmdSym(v, o);
      const cmd: Command = readCmd(v, o);
      if (journal !== null) journal.pushCmd(iseq, sym, cmd); // journal-before-apply
      nCommands++;
      core.apply(sym, cmd, emit);
    } else {
      const cut = cmdIseq(v, o), arg = cmdArg(v, o);
      // the new segment starts at this cut, before the snapshot is reported
      if (ctl === Control.Checkpoint && journal !== null && d.journal !== null)
        journal.rotate(openSegment(d.journal.dir, d.journal.format, "cmd", d.partition, d.partitions, d.book, cut));
      if (ctl === Control.Snapshot || ctl === Control.Checkpoint) {
        const blocks: Block[] = [];
        core.snapshotBlocks(blocks);
        d.port.postMessage({ op: arg, cut, blocks });
      }
      if (ctl === Control.Shutdown) {
        stop = true;
        if (journal !== null) {
          const e = journal.finish();
          if (e !== undefined) throw new Error(`journal: ${e}`);
        }
      } else {
        force = true;
      }
      const s = out.stage();
      if (s >= 0) writeEvtCtl(out.view, s, cut, ctl, arg);
    }
    if (eob) out.commit();
  };
  let lastHandoff = performance.now();
  for (;;) {
    force = false;
    const n = inbox.poll(h);
    if (n > 0) {
      counters[0] = nCommands; // a single aligned float64 store: readers see old or new
      counters[1] = nEvents;
    }
    if (stop || inbox.isAlerted()) break;
    if (journal !== null && journal.pending() > 0 && (force || (n === 0 && performance.now() - lastHandoff >= 0.05))) {
      journal.handOff();
      lastHandoff = performance.now();
    }
    if (n === 0) inbox.idle();
    else inbox.resetIdle();
  }
  out.commit();
}

if (!isMainThread && workerData !== null && workerData !== undefined && (workerData as Common).role !== undefined) {
  const d = workerData as Common;
  try {
    if (d.role === "router") router(d as RouterData);
    else if (d.role === "engine") engine(d as EngineData);
    else ioLoop((d as IoData).shared, d.alert);
  } catch (e) {
    recordFailure(d.fail, d.alert, `${d.name} thread failed: ${(e as Error).message ?? e}`);
  } finally {
    const ex = new Int32Array(d.exited);
    Atomics.store(ex, d.index, 1);
    Atomics.notify(ex, d.index);
    if (d.role === "engine") (d as EngineData).port.close();
  }
}
