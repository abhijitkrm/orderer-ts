// Integration suite (orderer-rust tests/{partitions,journal_recovery,
// control,plugs}.rs, ported). One publishing thread: where the Rust tests use
// several producers, these interleave several streams from the owner thread.
import * as fs from "fs";
import * as path from "path";
import { Side, Tif, cancel, eventCanonical, newLimit } from "../src/matcher/types";
import { EvtMsg } from "../src/msg";
import { acks, collect, Egress } from "../src/egress";
import { u64 } from "../src/flat";
import { CMD_RECORD, CorruptJournal, JournalConfig, JournalFormat, fsyncEvery, fsyncEveryN, journalConfig, journalPath, readCmdDir, readEvtJournal } from "../src/journal";
import { Pipeline, PipelineError, Snapshot, Status } from "../src/pipeline";
import { recover } from "../src/recover";
import { CORES } from "../src/core";
import { PartitionMap, hashPartition } from "../src/routing";
import { bySymbol, check, concat, dense, eq, fuzzCfg, fuzzCorpus, lines, referenceLines, referenceSnapshot, runAll, runPipeline, scratch, sleepMs } from "./t";

const CFG = fuzzCfg();

// ---- partitions ------------------------------------------------------------------------------

function everyPartitionCountMatchesReference(): void {
  for (let seed = 1; seed <= 4; seed++) {
    const cmds = fuzzCorpus(seed, 4000, 8);
    const ref = referenceLines(CFG, cmds);
    for (const P of [1, 2, 3, 4, 7]) {
      const parts = runPipeline(CFG, cmds, P);
      if (P === 1) check(eq(parts[0], ref), `seed ${seed}: P=1 is the plain engine stream`);
      const all = concat(parts);
      check(dense(all), "seq density");
      check(bySymbol(all) === bySymbol(ref), `seed ${seed} P=${P}`);
      parts.forEach((ls, q) => ls.forEach((l) => check(hashPartition(u64(l, "symbol")!, P) === q, `routing ${l}`)));
    }
  }
}

function fuzzRunsAreDeterministic(): void {
  for (let seed = 1; seed <= 4; seed++) {
    const cmds = fuzzCorpus(seed, 3000, 8);
    check(eq(runPipeline(CFG, cmds, 4), runPipeline(CFG, cmds, 4)), `seed ${seed}`);
  }
}

function partitionTableRoutesSymbols(): void {
  const cmds = fuzzCorpus(11, 3000, 8);
  const m = PartitionMap.make(3, Array.from({ length: 8 }, (_, s) => [s, s === 5 ? 0 : 2] as [number, number]));
  const [f, h] = collect(true);
  const p = Pipeline.builder().bookConfig(CFG).partitionMap(m).egress(f).build();
  p.publishBatch(cmds);
  p.drain();
  p.shutdown();
  const parts = h.take().map(lines);
  check(parts[1].length === 0);
  parts[0].forEach((l) => check(u64(l, "symbol") === 5, l));
  check(bySymbol(concat(parts)) === bySymbol(referenceLines(CFG, cmds)));
}

function interleavedStreamsPreservePerSymbolOrder(): void {
  const cmds = fuzzCorpus(3, 20000, 16);
  const [f, h] = collect(true);
  const p = Pipeline.builder().bookConfig(CFG).partitions(4).ringSizes(256, 64, 64).egress(f).build();
  const streams = [0, 1, 2, 3].map((k) => cmds.filter(([s]) => s % 4 === k));
  const pos = [0, 0, 0, 0];
  for (let turn = 0; pos.some((x, k) => x < streams[k].length); turn++) {
    const k = turn % 4, s = streams[k], n = Math.min(7, s.length - pos[k]);
    if (n <= 0) continue;
    if (n % 2 === 0) p.publishBatch(s, pos[k], pos[k] + n);
    else for (let j = 0; j < n; j++) p.publish(s[pos[k] + j][0], s[pos[k] + j][1]);
    pos[k] += n;
  }
  p.drain();
  p.shutdown();
  check(bySymbol(lines(h.listing())) === bySymbol(referenceLines(CFG, cmds)));
}

// ---- journals + recovery ---------------------------------------------------------------------

const jcfg = (dir: string, f: JournalFormat): JournalConfig => ({ ...journalConfig(dir, f), fsync: fsyncEveryN(64) });

function journalsSnapshotAndRecoveryRoundTrip(): void {
  for (const fmt of ["jsonl", "binary"] as JournalFormat[]) {
    for (const P of [1, 3]) {
      const dir = scratch(`jr-${fmt}-${P}`);
      const cmds = fuzzCorpus(21 + P, 5000, 8);
      const cut = 2000;
      const [f, h] = collect(true);
      const p = Pipeline.builder().bookConfig(CFG).partitions(P).ringSizes(512, 128, 128).journal(jcfg(dir, fmt)).egress(f).build();
      p.publishBatch(cmds, 0, cut);
      const snap = p.snapshot();
      p.publishBatch(cmds, cut);
      p.shutdown();
      const allRef = referenceLines(CFG, cmds), prefixLen = referenceLines(CFG, cmds.slice(0, cut)).length;
      check(snap.iseq === cut, `cut ${snap.iseq}`);
      check(snap.body === referenceSnapshot(CFG, cmds, cut), "snapshot body");
      const j = readCmdDir(dir, fmt);
      check(j.header.partitions === P, "journal header");
      const merged = j.partitions.flatMap((rs, q) => rs.map((r) => (check(hashPartition(r.sym, P) === q), r)));
      merged.sort((a, b) => a.iseq - b.iseq);
      check(merged.length === cmds.length);
      check(merged.every((r, i) => r.iseq === i + 1 && r.sym === cmds[i][0] && eq(r.cmd, cmds[i][1])), "records");
      const collected = h.take();
      for (let q = 0; q < P; q++) check(eq(readEvtJournal(journalPath(dir, "evt", q, fmt), fmt), lines(collected[q])), `evt-${q}`);
      for (const rp of [P, 2]) {
        const replayed: string[] = [];
        const rec = recover(CORES.fifo, CFG, PartitionMap.make(rp), snap, { dir, format: fmt }, (_q, s, seq, ev) => replayed.push(eventCanonical(seq, ev, s)));
        check(rec.snapshotIseq === cut && rec.lastIseq === cmds.length && rec.replayed === cmds.length - cut, "recovery counts");
        check(eq(allRef.slice(prefixLen), replayed), `recover P=${P} → ${rp}`);
      }
    }
  }
}

function recoveredPipelineResumesAndAppends(): void {
  const dir = scratch("resume");
  const j = jcfg(dir, "binary");
  const cmds = fuzzCorpus(77, 4000, 6);
  let p = Pipeline.builder().bookConfig(CFG).partitions(2).journal(j).build();
  p.publishBatch(cmds, 0, 2500);
  p.shutdown();
  const m = PartitionMap.make(2);
  const rec = recover(CORES.fifo, CFG, m, undefined, { dir, format: "binary" }, () => {});
  check(rec.lastIseq === 2500);
  const [f, h] = collect(true);
  p = Pipeline.builder().bookConfig(rec.book).partitionMap(m).journal({ ...j, append: true }).egress(f).initial(rec.initial()).build();
  p.publishBatch(cmds, 2500);
  p.drain();
  const snap = p.snapshot();
  p.shutdown();
  const allRef = referenceLines(CFG, cmds), prefixLen = referenceLines(CFG, cmds.slice(0, 2500)).length;
  check(bySymbol(lines(h.listing())) === bySymbol(allRef.slice(prefixLen)), "resumed stream");
  check(snap.iseq === 4000, "iseq resumed");
  check(snap.body === referenceSnapshot(CFG, cmds, 4000), "snapshot after resume");
  check(readCmdDir(dir, "binary").partitions.flat().length === 4000, "appended journals hold the whole history");
}

function tornAndCorruptJournalsAreErrors(): void {
  const cmds = fuzzCorpus(8, 500, 1);
  for (const fmt of ["jsonl", "binary"] as JournalFormat[]) {
    const dir = scratch(`torn-${fmt}`);
    const p = Pipeline.builder().bookConfig(CFG).journal(jcfg(dir, fmt)).build();
    p.publishBatch(cmds);
    p.shutdown();
    const pth = journalPath(dir, "cmd", 0, fmt);
    const good = fs.readFileSync(pth);
    const expectCorrupt = (b: Buffer, what: string) => {
      fs.writeFileSync(pth, b);
      let threw = false;
      try {
        readCmdDir(dir, fmt);
      } catch (e) {
        threw = e instanceof CorruptJournal;
        check((e as Error).message.includes(what), (e as Error).message);
      }
      check(threw, what);
    };
    expectCorrupt(good.subarray(0, good.length - 7), "torn");
    let back: Buffer;
    if (fmt === "binary") {
      back = Buffer.from(good);
      back.fill(0, back.length - CMD_RECORD, back.length - CMD_RECORD + 8);
      back[back.length - CMD_RECORD] = 1;
    } else {
      back = Buffer.concat([good, Buffer.from('{"cmd":"cancel","symbol":0,"order_id":1,"iseq":3}\n')]);
    }
    expectCorrupt(back, "iseq");
    const hdr = Buffer.from(good);
    hdr[2] = 35;
    expectCorrupt(hdr, "");
    fs.writeFileSync(pth, good);
    try {
      readCmdDir(dir, fmt);
    } catch (e) {
      check(false, `good journal rejected: ${e}`);
    }
  }
}

// ---- controls --------------------------------------------------------------------------------

function snapshotsInterleavedWithLoadAreCleanCuts(): void {
  const cmds = fuzzCorpus(31, 30000, 8);
  const p = Pipeline.builder().bookConfig(CFG).partitions(3).ringSizes(256, 64, 64).build();
  const snaps: Snapshot[] = [];
  for (let i = 0; i < cmds.length; i += 5000) {
    p.publishBatch(cmds, i, i + 5000);
    snaps.push(p.snapshot()); // cut while engines are still busy with the batch
  }
  p.shutdown();
  for (const s of snaps) check(s.body === referenceSnapshot(CFG, cmds, s.iseq), `cut at ${s.iseq}`);
  check(snaps[snaps.length - 1].iseq === cmds.length);
}

function shutdownIsIdempotentAndClosesPublishing(): void {
  const p = Pipeline.builder().partitions(2).build();
  p.publish(1, newLimit(1, Side.Bid, 10, 1, Tif.Gtc));
  p.shutdown();
  p.shutdown();
  check(p.publish(1, cancel(1)) === Status.Closed && p.tryPublish(1, cancel(1)) === Status.Closed && p.publishBatch([[1, cancel(1)]]) === Status.Closed);
  let threw = false;
  try {
    p.drain();
  } catch (e) {
    threw = e instanceof PipelineError && e.kind === "closed";
  }
  check(threw, "drain after shutdown");
}

function everyOkPublishBeforeShutdownIsApplied(): void {
  const [f, h] = collect(true);
  const p = Pipeline.builder().core("noop").partitions(2).ringSizes(64, 16, 16).egress(f).build();
  let accepted = 0;
  for (let i = 0; i < 5000; i++) if (p.publish(i % 3, cancel(i)) === Status.Ok) accepted++;
  p.shutdown();
  check(lines(h.listing()).length === accepted && accepted === 5000, "Ok ⇒ applied");
}

function acksWaitForFsync(): void {
  const cmds = fuzzCorpus(4, 2000, 6);
  const total = referenceLines(CFG, cmds).length;
  for (const noneBeforeShutdown of [true, false]) {
    const j = { ...jcfg(scratch("acks"), "binary" as JournalFormat), events: false };
    j.fsync = noneBeforeShutdown ? fsyncEvery(3_600_000_000) : { mode: "everyN", n: 2 ** 40, idleUs: 20_000 };
    let acked = 0;
    const p = Pipeline.builder().bookConfig(CFG).partitions(2).journal(j).egress(acks(() => acked++)).build();
    p.publishBatch(cmds);
    p.drain();
    if (noneBeforeShutdown) {
      sleepMs(100);
      p.pump();
      check(acked === 0 && p.durableIseq(0) === 0 && p.durableIseq(1) === 0, "acked before any fsync");
    } else {
      const deadline = performance.now() + 20_000;
      while (acked < total && performance.now() < deadline) {
        p.pump();
        sleepMs(5);
      }
      check(Math.max(p.durableIseq(0), p.durableIseq(1)) === cmds.length, "durable watermark");
    }
    p.shutdown();
    check(acked === total, `acked ${acked} of ${total}`);
  }
}

const slow = (): Egress => ({ onEvent: () => { for (const until = performance.now() + 0.02; performance.now() < until; ); } });

function tinyRingsAndSlowEgressBlockWithoutLoss(): void {
  const cmds = fuzzCorpus(17, 3000, 4);
  const [f, h] = collect(true);
  const p = Pipeline.builder().bookConfig(CFG).partitions(2).ringSizes(2, 2, 2).egress(slow).egress(f).build();
  for (const [s, c] of cmds) check(p.publish(s, c) === Status.Ok);
  p.drain();
  p.shutdown();
  check(bySymbol(lines(h.listing())) === bySymbol(referenceLines(CFG, cmds)));
}

function tryPublishShedsAtTheEdgeOnly(): void {
  const [f, h] = collect(true);
  const p = Pipeline.builder().core("noop").partitions(1).ringSizes(4, 2, 2).egress(slow).egress(f).build();
  let ok = 0, full = 0;
  for (let i = 0; i < 2000; i++) {
    const s = p.tryPublish(1, cancel(i));
    if (s === Status.Ok) ok++;
    else if (s === Status.Full) full++;
  }
  p.drain();
  p.shutdown();
  const got = lines(h.listing());
  check(full > 0 && got.length === ok && dense(got), `ok ${ok} full ${full} delivered ${got.length}`);
}

// ---- plugs -----------------------------------------------------------------------------------

function noopCoreSeesEveryCommand(): void {
  const cmds = fuzzCorpus(5, 5000, 8);
  const got = concat(runPipeline(CFG, cmds, 3, true, "noop"));
  check(got.length === cmds.length && dense(got));
}

function failingCoreFailsThePipelineInsteadOfHanging(): void {
  const p = Pipeline.builder().core(path.join(__dirname, "panic_core.js") + "#panicCore").partitions(2).build();
  p.publish(1, newLimit(1, Side.Bid, 10, 1, Tif.Gtc));
  p.publish(1, cancel(666));
  let failed = false;
  try {
    p.drain();
  } catch (e) {
    failed = e instanceof PipelineError && e.kind === "failed" && e.message.includes("engine");
  }
  check(failed, "drain fails");
  failed = false;
  try {
    p.shutdown();
  } catch (e) {
    failed = e instanceof PipelineError && e.kind === "failed";
  }
  check(failed, "shutdown fails");
}

function failingEgressFailsThePipeline(): void {
  const bad = (): Egress => ({ onEvent: (m: EvtMsg) => { if (m.ev.kind === "closed") throw new Error("plug broke"); } });
  const p = Pipeline.builder().partitions(1).egress(bad).build();
  p.publish(1, newLimit(1, Side.Bid, 10, 1, Tif.Gtc));
  p.publish(1, cancel(1));
  let failed = false;
  try {
    p.drain();
  } catch (e) {
    failed = e instanceof PipelineError && e.message.includes("plug broke");
  }
  check(failed, "egress failure surfaces");
  try {
    p.shutdown();
  } catch {
    /* expected */
  }
}

const tests: Array<[string, () => void]> = [
  ["every_partition_count_matches_reference_per_symbol", everyPartitionCountMatchesReference],
  ["fuzz_runs_are_deterministic_per_partition", fuzzRunsAreDeterministic],
  ["partition_table_routes_symbols", partitionTableRoutesSymbols],
  ["interleaved_streams_preserve_per_symbol_order", interleavedStreamsPreservePerSymbolOrder],
  ["journals_snapshot_and_recovery_round_trip", journalsSnapshotAndRecoveryRoundTrip],
  ["recovered_pipeline_resumes_and_appends", recoveredPipelineResumesAndAppends],
  ["torn_and_corrupt_journals_are_errors", tornAndCorruptJournalsAreErrors],
  ["snapshots_interleaved_with_load_are_clean_cuts", snapshotsInterleavedWithLoadAreCleanCuts],
  ["shutdown_is_idempotent_and_closes_publishing", shutdownIsIdempotentAndClosesPublishing],
  ["every_ok_publish_before_shutdown_is_applied", everyOkPublishBeforeShutdownIsApplied],
  ["acks_wait_for_fsync", acksWaitForFsync],
  ["tiny_rings_and_slow_egress_block_without_loss", tinyRingsAndSlowEgressBlockWithoutLoss],
  ["try_publish_sheds_at_the_edge_only", tryPublishShedsAtTheEdgeOnly],
  ["noop_core_sees_every_command", noopCoreSeesEveryCommand],
  ["failing_core_fails_the_pipeline_instead_of_hanging", failingCoreFailsThePipelineInsteadOfHanging],
  ["failing_egress_fails_the_pipeline", failingEgressFailsThePipeline],
];
runAll(tests);
