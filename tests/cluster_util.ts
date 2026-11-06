// Shared harness for the end-to-end cluster tests: real replicas over TCP with
// real files, several to a process.
import * as fs from "fs";
import * as net from "net";
import * as os from "os";
import * as path from "path";
import { Entry, NONE } from "../src/consensus";
import { Digest, DedupSm, RecorderSm, Replica, ReplicaConfig, StateMachine, defaultConfig } from "../src/cluster";
import { ProposeResult, Member } from "../src/cluster";
import { PipelineSm } from "../src/cluster/pipeline_sm";
import { BookConfig } from "../src/matcher/book";
import { IndexKind } from "../src/matcher/priceindex";

/// A narrow ladder: matcher-ts's default book is ~90 MB per symbol, which several replicas x symbols in one
/// test process would multiply. The replicas of a cluster only need to agree with each other.
export const testBook = (): BookConfig => ({ priceMin: 0, priceMax: 20_000, maxOrders: 65_536, index: IndexKind.Ladder });
import { check } from "./t";

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function until(cond: () => boolean | Promise<boolean>, ms: number, what: string): Promise<boolean> {
  const end = Date.now() + ms;
  while (!(await cond())) {
    if (Date.now() > end) { check(false, "timed out: ", what); return false; }
    await sleep(10);
  }
  return true;
}

export const payload = (i: number): Buffer => {
  const b = Buffer.alloc(8);
  b.writeUInt32LE(i >>> 0, 0);
  return b;
};
export const idOf = (p: Uint8Array): number => Buffer.from(p.buffer, p.byteOffset, p.length).readUInt32LE(0);

/// A RecorderSm whose state the test can read, with a corruption and a delay knob.
export class Shared {
  sm = new RecorderSm();
  corruptAt = 0;
  applyDelayMs = 0;
  payloads(): Uint8Array[] { return this.sm.applied.map((a) => a.data); }
  count(): number { return this.sm.applied.length; }
}

export class Probe implements StateMachine {
  constructor(private readonly s: Shared) {}
  apply(i: number, e: Entry): void {
    if (this.s.applyDelayMs > 0) for (const end = performance.now() + this.s.applyDelayMs; performance.now() < end; ); // a slow apply, on this thread
    if (this.s.corruptAt === i && e.data.length > 0) {
      const d = new Uint8Array(e.data);
      d[0] ^= 0xff;
      e = { term: e.term, data: d };
    }
    this.s.sm.apply(i, e);
  }
  snapshot(i: number): Uint8Array { return this.s.sm.snapshot(); }
  restore(i: number, d: Uint8Array): string | undefined { return this.s.sm.restore(i, d); }
  digest(): Digest { return this.s.sm.digest(); }
}

export async function freeAddrs(n: number): Promise<string[]> {
  const servers: net.Server[] = [];
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    const s = net.createServer();
    await new Promise<void>((r) => s.listen(0, "127.0.0.1", () => r()));
    servers.push(s);
    out.push(`127.0.0.1:${(s.address() as net.AddressInfo).port}`);
  }
  await Promise.all(servers.map((s) => new Promise<void>((r) => s.close(() => r()))));
  return out;
}

export interface ClusterOpts {
  tune?: (c: ReplicaConfig) => void;
  dedup?: boolean;
  pipeline?: boolean;
}

export class TestCluster {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), "orderer-ts-e2e-"));
  members: Member[] = [];
  nodes: Array<Replica | undefined> = [];
  shared: Shared[] = [];
  pipes: Array<PipelineSm | undefined> = [];

  private constructor(readonly o: ClusterOpts) {}

  static async create(n: number, o: ClusterOpts = {}): Promise<TestCluster> {
    const c = new TestCluster(o);
    (await freeAddrs(n)).forEach((addr, i) => {
      c.members.push({ id: i, addr });
      c.nodes.push(undefined);
      c.shared.push(new Shared());
      c.pipes.push(undefined);
    });
    for (let i = 0; i < n; i++) await c.start(i);
    return c;
  }

  config(i: number): ReplicaConfig {
    const cfg = defaultConfig(i, this.members, path.join(this.dir, `n${i}`));
    cfg.tickMs = 5;
    cfg.consensus = { electionTicks: 20, heartbeatTicks: 2, maxBatch: 256, preVote: true };
    cfg.fsync = false;
    cfg.proposeTimeoutMs = 3000;
    this.o.tune?.(cfg);
    return cfg;
  }

  makeSm(i: number): StateMachine {
    let sm: StateMachine;
    if (this.o.pipeline) sm = this.pipes[i] = new PipelineSm(2, testBook());
    else sm = new Probe(this.shared[i]);
    return this.o.dedup || this.o.pipeline ? new DedupSm(sm) : sm;
  }

  async start(i: number): Promise<void> {
    this.nodes[i] = await Replica.start(this.config(i), this.makeSm(i));
  }

  private closePipe(i: number): void { this.pipes[i]?.close(); this.pipes[i] = undefined; }
  async crash(i: number): Promise<void> { await this.nodes[i]!.crash(); this.nodes[i] = undefined; this.closePipe(i); }
  async stop(i: number): Promise<void> { await this.nodes[i]!.stop(); this.nodes[i] = undefined; this.closePipe(i); }
  async restart(i: number): Promise<void> { this.shared[i] = new Shared(); await this.start(i); }

  async close(): Promise<void> {
    for (let i = 0; i < this.nodes.length; i++) if (this.nodes[i]) await this.crash(i);
    fs.rmSync(this.dir, { recursive: true, force: true });
  }

  leader(): number {
    let best = -1, bt = 0;
    this.nodes.forEach((r, i) => {
      if (!r) return;
      const s = r.status();
      if (s.role === "Leader" && (best < 0 || s.term > bt)) { best = i; bt = s.term; }
    });
    return best;
  }

  async waitLeader(ms = 5000): Promise<number> {
    await until(() => this.leader() >= 0, ms, `no leader within ${ms}ms`);
    return this.leader();
  }

  /// Propose via whichever node leads, retrying redirects. Timeout and unknown are returned.
  async propose(p: Uint8Array): Promise<ProposeResult> {
    const end = Date.now() + 10000;
    for (;;) {
      const l = this.leader();
      if (l < 0) {
        if (Date.now() > end) return { kind: "stopped", index: 0, hint: NONE };
        await sleep(10);
        continue;
      }
      const r = await this.nodes[l]!.propose(p);
      if (r.kind === "notleader" || r.kind === "busy" || r.kind === "stopped") {
        if (Date.now() > end) return r;
        await sleep(5);
        continue;
      }
      return r;
    }
  }

  async mustPropose(p: Uint8Array): Promise<void> {
    const r = await this.propose(p);
    check(r.kind === "ok", "proposal failed: ", JSON.stringify(r));
  }

  waitApplied(who: number[], atLeast: number, ms = 10000): Promise<boolean> {
    return until(() => who.every((i) => this.shared[i].count() >= atLeast), ms, `nodes did not apply ${atLeast}`);
  }

  /// Wait until every running node has applied the same, stable sequence.
  async waitConverged(ms = 15000): Promise<Uint8Array[]> {
    const snap = () => this.nodes.flatMap((r, i) => (r ? [this.shared[i].payloads()] : []));
    const same = (v: Uint8Array[][]) => v.every((x) => eqp(x, v[0]));
    const end = Date.now() + ms;
    for (;;) {
      const a = snap();
      if (same(a)) {
        await sleep(100);
        const b = snap();
        if (same(b) && b.length === a.length && (a.length === 0 || eqp(a[0], b[0]))) return a.length === 0 ? [] : a[0];
      }
      if (Date.now() > end) { check(false, "replicas did not converge"); return []; }
      await sleep(20);
    }
  }
}

export const eqp = (a: Uint8Array[], b: Uint8Array[]): boolean =>
  a.length === b.length && a.every((x, i) => Buffer.compare(x, b[i]) === 0);

export const seq = (from: number, to: number): Uint8Array[] => Array.from({ length: to - from }, (_, k) => payload(from + k));

export const expectSeq = (got: Uint8Array[], from: number, to: number, why: string) =>
  check(eqp(got, seq(from, to)), why, ": got ", got.length, " entries, want ", from, "..", to);
