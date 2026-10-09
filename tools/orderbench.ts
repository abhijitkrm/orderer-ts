//! orderbench — spec/BENCH.md protocol.
//!   orderbench <prefix> --mode core [--tag NAME]
//!   orderbench <prefix> --mode pipe --partitions P [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME]
//! orderer-ts tuning flags (listed in the config column when set):
//!   --core fifo|noop  --waits relaxed|low  --batch N  --ingress N --inbox N --outbox N
//!   --events on|off   --baseline OPS (core untimed ops/s, for eff)  --warmups N (JIT, default 3)
//! --producers N > 1 publishes from N worker threads (symbol % N streams)
//! through Handles while the owner pumps egress.
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { execSync } from "child_process";
import { Worker } from "worker_threads";
import { Engine } from "../src/matcher/engine";
import { OrderBook } from "../src/matcher/book";
import { NullSink } from "../src/matcher/sink";
import { metrics } from "../src/egress";
import { fsyncEveryN, fsyncNever, journalConfig, JournalConfig } from "../src/journal";
import { Args, Corpus, die, fail, loadCorpus } from "../src/harness";
import { Pipeline, Waits, lowLatencyWaits, relaxedWaits } from "../src/pipeline";

/// Collect garbage before a measured pass (the wrapper runs node --expose-gc).
const gc = (globalThis as { gc?: () => void }).gc ?? (() => {});

interface Row {
  ops: number;
  wallNs: number;
  lat: Float64Array;
  untimed?: number;
}

function coreMode(setup: Corpus, run: Corpus, warmups: number): Row {
  const sink = new NullSink();
  const make = () => {
    if (setup.engine) {
      const e = new Engine(setup.book);
      return (s: number, c: (typeof run.cmds)[0][1]) => e.apply(s, c, sink);
    }
    const b = new OrderBook(setup.book);
    return (_s: number, c: (typeof run.cmds)[0][1]) => b.apply(c, sink);
  };
  const all = (t: ReturnType<typeof make>, c: Corpus, n = c.cmds.length) => {
    for (let i = 0; i < n; i++) t(c.cmds[i][0], c.cmds[i][1]);
  };
  for (let w = 0; w < warmups; w++) { // spec warmup (setup + 10% of run), repeated for the JIT
    const t = make();
    all(t, setup);
    all(t, run, w === warmups - 1 ? Math.floor(run.cmds.length / 10) : run.cmds.length);
  }
  const lat = new Float64Array(run.cmds.length);
  let wallNs: number;
  { // timed per op (matcher protocol)
    const t = make();
    all(t, setup);
    gc();
    const wall = performance.now();
    for (let i = 0; i < run.cmds.length; i++) {
      const t0 = performance.now();
      t(run.cmds[i][0], run.cmds[i][1]);
      lat[i] = (performance.now() - t0) * 1e6;
    }
    wallNs = (performance.now() - wall) * 1e6;
  }
  let untimed: number;
  { // untimed: the scaling gate's denominator (spec/BENCH.md 1.1)
    const t = make();
    all(t, setup);
    gc();
    const cmds = run.cmds;
    const wall = performance.now();
    for (let i = 0; i < cmds.length; i++) t(cmds[i][0], cmds[i][1]);
    untimed = run.cmds.length / ((performance.now() - wall) / 1000);
  }
  if (sink.acc === 42) process.stderr.write("");
  return { ops: run.cmds.length, wallNs, lat, untimed };
}

interface PipeOpts {
  producers: number;
  partitions: number;
  batch: number;
  journal?: JournalConfig;
  waits: Waits;
  core: string;
  rings: [number, number, number];
}

function build(o: PipeOpts, book: Corpus["book"], m?: ReturnType<typeof metrics>[0]): Pipeline {
  const b = Pipeline.builder().core(o.core).bookConfig(book).partitions(o.partitions).waits(o.waits).ringSizes(...o.rings);
  if (o.journal !== undefined) b.journal(o.journal);
  if (m !== undefined) b.egress(m);
  return b.build();
}

function pipeMode(setup: Corpus, run: Corpus, o: PipeOpts, warmups: number): Row {
  for (let w = 0; w < warmups; w++) { // warmup on throwaway pipelines
    const p = build(o, setup.book);
    p.publishBatch(setup.cmds);
    p.publishBatch(run.cmds, 0, w === warmups - 1 ? Math.floor(run.cmds.length / 10) : run.cmds.length);
    p.drain();
    p.shutdown();
  }
  const [mf, results] = metrics(run.cmds.length + 1024);
  const p = build(o, setup.book, mf);
  p.publishBatch(setup.cmds);
  p.drain();
  gc();
  p.setTimestamps(true);
  let wall: number;
  if (o.producers <= 1) {
    wall = performance.now();
    for (let i = 0; i < run.cmds.length; i += o.batch) p.publishBatch(run.cmds, i, Math.min(i + o.batch, run.cmds.length));
  } else {
    const ctl = new Int32Array(new SharedArrayBuffer(12)); // ready, start, done
    const streams: Array<Array<(typeof run.cmds)[0]>> = Array.from({ length: o.producers }, () => []);
    for (const c of run.cmds) streams[c[0] % o.producers].push(c);
    for (const st of streams) {
      new Worker(path.join(__dirname, "producer.js"), { workerData: { desc: p.handleDescriptor(), cmds: st, batch: o.batch, ctl: ctl.buffer } }).unref();
    }
    p.pumpWhile(() => Atomics.load(ctl, 0) < o.producers); // workers started, not timed
    wall = performance.now();
    Atomics.store(ctl, 1, 1);
    Atomics.notify(ctl, 1);
    p.pumpWhile(() => Atomics.load(ctl, 2) < o.producers);
  }
  p.drain();
  const wallNs = (performance.now() - wall) * 1e6;
  p.setTimestamps(false);
  p.shutdown();
  const total = results.reduce((s, m) => s + m.samples, 0);
  const lat = new Float64Array(total);
  let at = 0;
  for (const m of results) {
    lat.set(m.latencies.subarray(0, m.samples), at);
    at += m.samples;
  }
  return { ops: run.cmds.length, wallNs, lat };
}

const pct = (v: Float64Array, p: number) => (v.length === 0 ? 0 : v[Math.min(Math.ceil((v.length - 1) * p), v.length - 1)]);

function cpu(): string {
  try {
    const s = execSync("sysctl -n machdep.cpu.brand_string", { stdio: ["ignore", "pipe", "ignore"] }).toString().trim();
    if (s !== "") return s;
  } catch { /* not macOS */ }
  return os.cpus()[0]?.model ?? "unknown cpu";
}

const usage = "orderbench <prefix> --mode core|pipe [--partitions P] [--producers N] [--journal binary|jsonl|off] " +
  "[--journal-dir DIR] [--fsync N] [--tag NAME] [--core fifo|noop] [--waits relaxed|low] [--batch N] " +
  "[--ingress N] [--inbox N] [--outbox N] [--events on|off] [--baseline OPS] [--warmups N]";
const a = new Args(process.argv.slice(2), usage, ["--mode", "--partitions", "--producers", "--journal", "--journal-dir",
  "--fsync", "--tag", "--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events", "--baseline", "--warmups"], []);
if (a.positional.length !== 1) die(usage);
const prefix = a.positional[0];
const tag = a.get("--tag") ?? path.basename(prefix);
const setup = loadCorpus(prefix + ".setup.cmd.jsonl");
const run = loadCorpus(prefix + ".run.cmd.jsonl");
const mode = a.get("--mode") ?? "core";
const warmups = Math.max(a.num("--warmups", 3, 100), 1);
const config: string[] = [];
let row: Row;
let P = "-", prod = "-";
if (mode === "core") {
  row = coreMode(setup, run, warmups);
} else if (mode === "pipe") {
  const o: PipeOpts = {
    producers: Math.max(a.num("--producers", 1, 63), 1),
    partitions: a.num("--partitions", 1, 1024), batch: Math.max(a.num("--batch", 64, 1 << 20), 1),
    waits: lowLatencyWaits(), core: a.get("--core") ?? "fifo",
    rings: [a.num("--ingress", 1 << 14, 1 << 30), a.num("--inbox", 1 << 12, 1 << 30), a.num("--outbox", 1 << 13, 1 << 30)],
  };
  const fsync = a.num("--fsync", 1024, Number.MAX_SAFE_INTEGER);
  const jm = a.get("--journal") ?? "binary";
  const tmp = path.join(os.tmpdir(), `orderbench-ts-${process.pid}`);
  if (jm === "binary" || jm === "jsonl") {
    const j = journalConfig(a.get("--journal-dir") ?? tmp, jm);
    j.fsync = fsync > 0 ? fsyncEveryN(fsync) : fsyncNever();
    j.events = a.get("--events") === "on";
    o.journal = j;
  } else if (jm !== "off") die(`--journal: unknown mode ${jm}`);
  const w = a.get("--waits") ?? "low";
  if (w === "relaxed") o.waits = relaxedWaits();
  else if (w !== "low") die(`--waits: unknown ${w}`);
  if (o.core !== "fifo" && o.core !== "noop") die(`--core: unknown ${o.core}`);
  config.push(`journal=${jm} fsync=${fsync}`);
  for (const k of ["--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events", "--warmups"])
    if (a.get(k) !== undefined) config.push(`${k.slice(2)}=${a.get(k)}`);
  try {
    row = pipeMode(setup, run, o, warmups);
  } catch (e) {
    fail((e as Error).message);
  }
  fs.rmSync(tmp, { recursive: true, force: true });
  P = String(o.partitions);
  prod = String(o.producers);
} else {
  die(`--mode: unknown ${mode}`);
}
if (row.untimed !== undefined) config.push(`untimed=${Math.floor(row.untimed)}`);
row.lat.sort();
const opsS = row.ops / (row.wallNs / 1e9);
const mean = row.lat.length === 0 ? 0 : row.lat.reduce((s, v) => s + v, 0) / row.lat.length;
const base = a.get("--baseline");
const eff = mode === "pipe" && base !== undefined ? (opsS / (Number(P) * Number(base))).toFixed(2) : "";
const r = (x: number) => Math.round(x);
process.stdout.write(`| ${tag} | ${mode} | ${P} | ${prod} | ${row.ops} | ${r(opsS)} | ${eff} | ${r(mean)} | ${r(pct(row.lat, 0.5))} | ${r(pct(row.lat, 0.9))} | ${r(pct(row.lat, 0.99))} | ${r(pct(row.lat, 0.999))} | ${r(row.lat.length === 0 ? 0 : row.lat[row.lat.length - 1])} | ${config.join(" ")} |\n`);
process.stderr.write(`env: ${cpu()} / orderer-ts 0.2.0 / node ${process.version}\n`);
