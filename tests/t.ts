// Test helpers: a minimal runner, a seeded adversarial generator,
// single-Engine references, per-symbol views, pipeline runs.
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BookConfig } from "../src/matcher/book";
import { Engine } from "../src/matcher/engine";
import { IndexKind } from "../src/matcher/priceindex";
import { NullSink } from "../src/matcher/sink";
import { writeEngine } from "../src/matcher/snapshot";
import { Command, Side, Tif, cancel, eventCanonical, newLimit, newMarket, replace } from "../src/matcher/types";
import { collect } from "../src/egress";
import { u64 } from "../src/flat";
import { Pipeline } from "../src/pipeline";

let failures = 0, checks = 0, current = "";

export function check(ok: boolean, ...msg: unknown[]): void {
  checks++;
  if (!ok) {
    failures++;
    if (failures <= 20) console.log(`  FAIL [${current}] ${msg.join("")}`);
  }
}

export function runAll(tests: Array<[string, () => void]>): void {
  const t0 = performance.now();
  for (const [name, body] of tests) {
    current = name;
    const before = failures, s = performance.now();
    try {
      body();
    } catch (e) {
      failures++;
      console.log(`  FAIL [${name}] threw ${(e as Error).stack ?? e}`);
    }
    console.log(`${failures === before ? "ok  " : "FAIL"} ${name} (${Math.round(performance.now() - s)} ms)`);
  }
  console.log(`${tests.length} tests, ${checks} checks, ${failures} failures (${Math.round(performance.now() - t0)} ms)`);
  process.exitCode = failures === 0 ? 0 : 1;
}

export const slurp = (p: string) => fs.readFileSync(p, "utf8");
export const lines = (s: string) => (s === "" ? [] : s.replace(/\n$/, "").split("\n"));
export type Cmds = Array<[number, Command]>;

/// xorshift64* (the tools' generator family), in BigInt for exactness.
class Rng {
  private x: bigint;
  constructor(seed: bigint) {
    this.x = seed;
  }
  next(): bigint {
    const M = (1n << 64n) - 1n;
    this.x ^= this.x >> 12n;
    this.x ^= (this.x << 25n) & M;
    this.x ^= this.x >> 27n;
    return (this.x * 0x2545f4914f6cdd1dn) & M;
  }
  below(n: number): number {
    return Number(this.next() % BigInt(n));
  }
}

export const fuzzCfg = (): BookConfig => ({ priceMin: 1, priceMax: 200, maxOrders: 4096, index: IndexKind.Ladder });

/// Adversarial engine stream: crossing prices, small id space, every TIF, markets, ~5% malformed.
export function fuzzCorpus(seed: number, n: number, symbols: number): Cmds {
  const r = new Rng(((BigInt(seed) * 0x9e3779b97f4a7c15n) & ((1n << 64n) - 1n)) | 1n);
  const tifs = [Tif.Gtc, Tif.Gtc, Tif.Ioc, Tif.Fok, Tif.PostOnly];
  const out: Cmds = [];
  for (let i = 0; i < n; i++) {
    const sym = r.below(symbols), id = r.below(256);
    const price = r.below(20) === 0 ? [0, 201, -5][r.below(3)] : 90 + r.below(21);
    const qty = r.below(25) === 0 ? 0 : r.below(50) + 1;
    const k = r.below(10);
    let c: Command;
    if (k < 5) {
      const side = r.below(2) === 1 ? Side.Ask : Side.Bid;
      c = r.below(8) === 0 ? newMarket(id, side, qty) : newLimit(id, side, price, qty, tifs[r.below(5)]);
    } else if (k < 8) c = cancel(id);
    else c = replace(id, price, qty);
    out.push([sym, c]);
  }
  return out;
}

export function referenceLines(cfg: BookConfig, cmds: Cmds): string[] {
  const e = new Engine(cfg);
  const out: string[] = [];
  for (const [s, c] of cmds) e.applyTagged(s, c, (sym, seq, ev) => out.push(eventCanonical(seq, ev, sym)));
  return out;
}

export function referenceSnapshot(cfg: BookConfig, cmds: Cmds, n: number): string {
  const e = new Engine(cfg);
  const sink = new NullSink();
  for (const [s, c] of cmds.slice(0, n)) e.apply(s, c, sink);
  return writeEngine(e);
}

export function bySymbol(ls: string[]): string {
  const m = new Map<number, string[]>();
  for (const l of ls) {
    const s = u64(l, "symbol") ?? 0;
    if (!m.has(s)) m.set(s, []);
    m.get(s)!.push(l);
  }
  return JSON.stringify([...m.entries()].sort((a, b) => a[0] - b[0]));
}

export function dense(ls: string[]): boolean {
  const next = new Map<number, number>();
  for (const l of ls) {
    const s = u64(l, "symbol") ?? 0, want = (next.get(s) ?? 0) + 1;
    next.set(s, want);
    if (u64(l, "seq") !== want) return false;
  }
  return true;
}

/// Per-partition canonical lines from a pipeline run.
export function runPipeline(cfg: BookConfig, cmds: Cmds, P: number, tagged = true, core = "fifo"): string[][] {
  const [f, h] = collect(tagged);
  const p = Pipeline.builder().core(core).bookConfig(cfg).partitions(P).ringSizes(1 << 10, 1 << 8, 1 << 8).egress(f).build();
  p.publishBatch(cmds);
  p.drain();
  p.shutdown();
  return h.take().map(lines);
}

export const concat = (v: string[][]) => v.flat();
export const eq = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

export function scratch(name: string): string {
  const d = path.join(os.tmpdir(), `orderer-ts-test-${process.pid}`, name);
  fs.rmSync(d, { recursive: true, force: true });
  fs.mkdirSync(d, { recursive: true });
  return d;
}

export function sleepMs(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}
