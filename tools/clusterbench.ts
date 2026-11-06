//! clusterbench — throughput of the replicated path, over loopback TCP (the
//! TypeScript counterpart of orderer-rust's clusterbench, same flags and
//! output line).
//!
//!   clusterbench [--nodes N] [--groups G] [--sm noop|pipeline] [--partitions P]
//!                [--batch CMDS] [--producers K] [--window W]
//!                [--cmds TOTAL] [--fsync on|off] [--dir PATH] [--book small|default]
//!
//! G independent replicated groups of N nodes each (symbols sharded between
//! them). Every replica runs in its own worker thread, the way the other
//! languages give every replica its own threads; for each group, K producers
//! in the leader's worker keep W batches of CMDS commands in flight against
//! it. Everything shares one machine: use the numbers as a floor for what
//! dedicated hardware per replica would do. --book small (default) uses a
//! 20000-level ladder because matcher-ts's default book is ~90 MB per symbol.
import * as fs from "fs";
import * as net from "net";
import * as os from "os";
import * as path from "path";
import { isMainThread, parentPort, Worker, workerData } from "worker_threads";
import { DedupSm, PipelineSm, Replica, ShardMap, StateMachine, defaultConfig } from "../src/cluster";
import { put64 } from "../src/cluster/codec";
import { Entry } from "../src/consensus";
import { BookConfig, defaultConfig as defaultBook } from "../src/matcher/book";
import { IndexKind } from "../src/matcher/priceindex";

interface Opts {
  nodes: number; groups: number; sm: string; partitions: number; batch: number; producers: number; window: number;
  cmds: number; fsync: boolean; dir: string; book: string;
}

/// matcher-ts's default book allocates a 1M-level ladder per side per symbol (about 90 MB), which
/// 64 symbols x N replicas in one machine cannot hold; the bench's prices sit near 10000, so a
/// narrow ladder gives the same matching work. (All replicas of a cluster must share one config.)
const smallBook = (): BookConfig => ({ priceMin: 0, priceMax: 20_000, maxOrders: 65_536, index: IndexKind.Ladder });

class Noop implements StateMachine {
  apply(_i: number, _e: Entry): void {}
  snapshot(): Uint8Array { return new Uint8Array(0); }
  restore(): string | undefined { return undefined; }
  digest(): undefined { return undefined; }
}

interface WorkerArgs {
  o: Opts; g: number; i: number; members: Array<{ id: number; addr: string }>; syms: number[]; perProducer: number;
}

async function worker(): Promise<void> {
  const { o, g, i, members, syms, perProducer } = workerData as WorkerArgs;
  const cfg = defaultConfig(i, members, path.join(o.dir, `g${g}n${i}`));
  cfg.clusterId = 1000 + g;
  cfg.consensus = { electionTicks: 30, heartbeatTicks: 3, maxBatch: 512, preVote: true };
  cfg.tickMs = 10;
  cfg.fsync = o.fsync;
  cfg.maxPending = 1 << 20;
  cfg.proposeTimeoutMs = 60000;
  cfg.maxApplyBacklog = 1 << 20;
  const pipe = o.sm === "pipeline" ? new PipelineSm(o.partitions, o.book === "default" ? defaultBook() : smallBook()) : undefined;
  const sm = pipe ? new DedupSm(pipe) : new Noop();
  const r = await Replica.start(cfg, sm);
  parentPort!.postMessage({ ready: true });
  if (process.env.BENCH_DEBUG) {
    let last = performance.now(), role = "";
    setInterval(() => {
      const now = performance.now(), st = r.status();
      if (now - last > 150) process.stderr.write(`g${g}n${i}: loop stalled ${(now - last).toFixed(0)} ms (applied ${st.applied} commit ${st.commit})\n`);
      if (st.role !== role || st.halted) { role = st.role; process.stderr.write(`g${g}n${i}: ${st.role} term ${st.term} ${st.halted}\n`); }
      last = now;
    }, 20);
  }

  parentPort!.on("message", async (m: { cmd: string }) => {
    if (m.cmd === "status") {
      const s = r.status();
      parentPort!.postMessage({ status: { leader: s.role === "Leader", commit: s.commit, applied: s.applied } });
    } else if (m.cmd === "load") {
      let acked = 0, latMs = 0, batches = 0;
      await Promise.all(Array.from({ length: o.producers }, async (_, p) => {
        let nextId = (g * 64 + p + 1) * 2 ** 36;
        let x = (0x9e3779b9 ^ (g * 64 + p + 1)) >>> 0 || 1;
        let sent = 0;
        const q: Array<{ t: number; p: Promise<{ kind: string }> }> = [];
        while (sent < perProducer || q.length > 0) {
          while (sent < perProducer && q.length < o.window) {
            const b = Buffer.alloc(5 + o.batch * 32);
            b[0] = 1;
            b.writeUInt32LE(o.batch, 1);
            for (let k = 0, at = 5; k < o.batch; k++, at += 32) {
              x ^= x << 13; x >>>= 0; x ^= x >>> 17; x ^= x << 5; x >>>= 0;
              b.writeUInt32LE(syms[x % syms.length], at);
              b[at + 4] = 1; // new
              b[at + 5] = x & 1; // side
              put64(b, at + 8, ++nextId);
              put64(b, at + 16, 10_000 + ((x >>> 8) % 8) - 4);
              put64(b, at + 24, 1 + ((x >>> 20) % 10));
            }
            q.push({ t: performance.now(), p: r.propose(b) });
            sent += o.batch;
          }
          const h = q.shift()!;
          const res = await h.p;
          if (res.kind === "ok") { acked += o.batch; latMs += performance.now() - h.t; batches++; }
          else if (res.kind === "busy") await new Promise((rs) => setTimeout(rs, 1));
          else { parentPort!.postMessage({ error: `proposal failed: ${res.kind}` }); return; }
        }
      }));
      parentPort!.postMessage({ done: { acked, latMs, batches } });
    } else if (m.cmd === "exit") {
      await r.crash();
      pipe?.close();
      process.exit(0);
    }
  });
}

async function freePorts(n: number): Promise<string[]> {
  const ss: net.Server[] = [];
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    const s = net.createServer();
    await new Promise<void>((r) => s.listen(0, "127.0.0.1", () => r()));
    ss.push(s);
    out.push(`127.0.0.1:${(s.address() as net.AddressInfo).port}`);
  }
  await Promise.all(ss.map((s) => new Promise<void>((r) => s.close(() => r()))));
  return out;
}

let workdir = "";
function die(msg: string): never {
  process.stderr.write(`${msg}\n`);
  if (workdir) fs.rmSync(workdir, { recursive: true, force: true });
  process.exit(2);
}

async function main(): Promise<void> {
  const o: Opts = { nodes: 3, groups: 1, sm: "noop", partitions: 2, batch: 1000, producers: 2, window: 8, cmds: 20_000_000, fsync: true, dir: "", book: "small" };
  const a = process.argv.slice(2);
  for (let i = 0; i < a.length; i++) {
    const v = () => a[++i] ?? die(`${a[i - 1]} needs a value`);
    const n = () => { const x = Number(v()); return Number.isFinite(x) ? x : die(`${a[i - 1]} wants a number`); };
    switch (a[i]) {
      case "--nodes": o.nodes = n(); break;
      case "--groups": o.groups = n(); break;
      case "--sm": o.sm = v(); break;
      case "--partitions": o.partitions = n(); break;
      case "--batch": o.batch = n(); break;
      case "--producers": o.producers = n(); break;
      case "--window": o.window = n(); break;
      case "--cmds": o.cmds = n(); break;
      case "--fsync": o.fsync = v() === "on"; break;
      case "--dir": o.dir = v(); break;
      case "--book": o.book = v(); break;
      default: die(`unknown argument ${a[i]}`);
    }
  }
  if (o.sm !== "noop" && o.sm !== "pipeline") die(`--sm ${o.sm}`);
  if (!o.dir) o.dir = path.join(os.tmpdir(), `clusterbench-ts-${process.pid}`);
  fs.rmSync(o.dir, { recursive: true, force: true });
  workdir = o.dir;

  const map = new ShardMap(o.groups);
  const perProducer = Math.floor(o.cmds / (o.groups * o.producers));
  const workers: Worker[][] = [];
  const waiters = new Map<Worker, (m: any) => void>();
  const ask = (w: Worker, msg: object, want: string): Promise<any> => new Promise((res) => {
    waiters.set(w, (m) => { if (m.error) die(m.error); if (want in m) { waiters.delete(w); res(m[want]); } });
    w.postMessage(msg);
  });

  for (let g = 0; g < o.groups; g++) {
    const members = (await freePorts(o.nodes)).map((addr, id) => ({ id, addr }));
    const syms: number[] = [];
    for (let s = 0; s < 4096 && syms.length < 64; s++) if (map.groupOf(s) === g) syms.push(s);
    const ws: Worker[] = [];
    for (let i = 0; i < o.nodes; i++) {
      const w = new Worker(__filename, { workerData: { o, g, i, members, syms, perProducer } satisfies WorkerArgs });
      w.on("message", (m) => { const f = waiters.get(w); if (f) f(m); });
      w.on("error", (e) => die(`worker failed: ${(e as Error).stack ?? e}`));
      ws.push(w);
    }
    workers.push(ws);
  }
  await Promise.all(workers.flat().map((w) => new Promise<void>((res) => waiters.set(w, (m) => { if (m.ready) { waiters.delete(w); res(); } }))));

  const status = async (w: Worker) => ask(w, { cmd: "status" }, "status") as Promise<{ leader: boolean; commit: number; applied: number }>;
  const leaders: number[] = [];
  const until = Date.now() + 20000;
  for (let g = 0; g < o.groups; g++) {
    for (;;) {
      let found = -1;
      for (let i = 0; i < o.nodes; i++) if ((await status(workers[g][i])).leader) found = i;
      if (found >= 0) { leaders.push(found); break; }
      if (Date.now() > until) die("no leader");
      await new Promise((r) => setTimeout(r, 10));
    }
  }

  const t0 = performance.now();
  const results = await Promise.all(workers.map((ws, g) => ask(ws[leaders[g]], { cmd: "load" }, "done"))) as Array<{ acked: number; latMs: number; batches: number }>;
  const secs = (performance.now() - t0) / 1000;
  const done = results.reduce((s, r) => s + r.acked, 0);
  const latMs = results.reduce((s, r) => s + r.latMs, 0);
  const nb = Math.max(results.reduce((s, r) => s + r.batches, 0), 1);

  // Every node of every group must apply what its group committed.
  const deadline = Date.now() + 180000;
  for (let g = 0; g < o.groups; g++) {
    const want = (await status(workers[g][leaders[g]])).commit;
    for (;;) {
      const sts = await Promise.all(workers[g].map(status));
      if (sts.every((s) => s.applied >= want)) break;
      if (Date.now() > deadline) die("followers did not catch up");
      await new Promise((r) => setTimeout(r, 5));
    }
  }
  const appliedSecs = (performance.now() - t0) / 1000;
  const f2 = (x: number) => x.toFixed(2);
  console.log(`groups=${o.groups} nodes=${o.nodes} sm=${o.sm} batch=${o.batch} producers=${o.producers}/group window=${o.window} fsync=${o.fsync ? "on" : "off"}: ` +
    `${f2(done / secs / 1e6)}M cmds/s acked (${f2(done / secs / 1e6 / o.groups)}M per group), ${f2(done / appliedSecs / 1e6)}M applied everywhere, ` +
    `${(done * 32 / 1e6 / secs).toFixed(0)} MB/s log payload, mean batch latency ${f2(latMs / nb)} ms, ${done} cmds in ${f2(secs)}s`);
  for (const w of workers.flat()) w.postMessage({ cmd: "exit" });
  await Promise.all(workers.flat().map((w) => new Promise<void>((r) => w.on("exit", () => r()))));
  fs.rmSync(o.dir, { recursive: true, force: true });
}

if (isMainThread) void main().then(() => process.exit(0));
else void worker();
