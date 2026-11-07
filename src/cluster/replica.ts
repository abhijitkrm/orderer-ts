// The replica runtime (port of orderer-rust's replica.rs and the C++/Go
// runtimes; spec/REPLICATION.md 13). One event-loop thread owns the protocol
// Node and executes its effects: WAL writes with an fsync that runs off the
// loop (group commit: everything queued during one fsync goes in the next),
// network sends, snapshots, and state machine applies. Acknowledgement is at
// commit.
//
// Deviation from the threaded runtimes: the state machine runs on the same
// thread as the protocol, in time slices (APPLY_SLICE_MS) between I/O polls.
// A backlog of applies therefore cannot starve heartbeats, but a single
// apply() that itself takes longer than the election timeout will; use a
// worker thread per replica (as clusterbench does) when that matters.
// Producers are pushed back with Busy when the apply backlog grows.

import * as path from "path";
import { Config, Disk, Entry, NONE, Node, NodeId, Persist, Role } from "../consensus";
import { Digest, deepDigestOf, sha256, StateMachine } from "./statemachine";
import { Filter, Inbound, Member, Transport } from "./transport";
import { SnapStore } from "./snapstore";
import { Wal, WalError } from "./wal";

const APPLY_SLICE_MS = 2;

export interface ReplicaConfig {
  id: NodeId;
  members: Member[];
  clusterId: number;
  dir: string;
  tickMs: number;
  consensus: Config;
  /// Snapshot and compact the log every N applied entries; 0 = never.
  compactEvery: number;
  /// Exchange state digests every N entries to detect divergence; 0 = off.
  digestEvery: number;
  /// false: tests and benchmarks only; loses durability.
  fsync: boolean;
  proposeTimeoutMs: number;
  maxPending: number;
  /// How far the state machine may lag the log before proposals get Busy.
  maxApplyBacklog: number;
  /// Pre-shared cluster key; undefined = no authentication.
  authKey?: Uint8Array;
}

export function defaultConfig(id: NodeId, members: Member[], dir: string): ReplicaConfig {
  return {
    id, members, clusterId: 1, dir, tickMs: 10,
    consensus: { electionTicks: 30, heartbeatTicks: 3, maxBatch: 256, preVote: true },
    compactEvery: 0, digestEvery: 0, fsync: true, proposeTimeoutMs: 5000, maxPending: 4096, maxApplyBacklog: 8192,
  };
}

export type ProposeKind = "ok" | "notleader" | "busy" | "timeout" | "superseded" | "unknown" | "stopped";

export interface ProposeResult {
  kind: ProposeKind;
  index: number;
  /// notleader: the leader this node last heard from, or NONE.
  hint: number;
}

export interface Status {
  id: NodeId;
  role: Role;
  term: number;
  leader: number;
  commit: number;
  applied: number;
  base: number;
  lastIndex: number;
  pending: number;
  halted: string; // empty = running
  digestsVerified: number;
}

export class StartError extends Error {}

type ApplyJob =
  | { kind: "apply"; index: number; entry: Entry }
  | { kind: "restore"; index: number }
  | { kind: "hash"; resolve: (r: { index: number; digest: Digest } | undefined) => void };

interface Pending {
  term: number;
  resolve: (r: ProposeResult) => void;
  deadline: number;
}

const res = (kind: ProposeKind, index = 0, hint = NONE): ProposeResult => ({ kind, index, hint });

export class Replica {
  readonly filter: Filter;
  private readonly node: Node;
  private readonly wal: Wal;
  private readonly snaps: SnapStore;
  private transport!: Transport;

  private stopped = false;
  private aborted = false;
  private halted = "";
  private ticker?: NodeJS.Timeout;
  private driveScheduled = false;
  private replicateWanted = false;

  private walQ: Persist[] = [];
  private walBusy = false;
  private walIdle?: () => void; // resolved when the in-flight fsync ends
  private readonly compactTokens = new Map<number, number>();
  private closeWal = false; // close the file once the in-flight fsync ends

  private applyQ: ApplyJob[] = [];
  private applyHead = 0;
  private applyScheduled = false;
  private smApplied: number;
  private readonly waiters: Array<{ index: number; resolve: () => void }> = [];

  private readonly pending = new Map<number, Pending>();
  private readonly digests = new Map<number, Map<NodeId, Digest>>();
  private digestsVerified = 0;

  private constructor(private readonly cfg: ReplicaConfig, private readonly sm: StateMachine, filter: Filter) {
    this.filter = filter;
    let disk: Disk;
    try {
      const o = Wal.open(path.join(cfg.dir, "wal"));
      this.wal = o.wal;
      disk = o.disk;
    } catch (e) {
      throw new StartError((e as Error).message);
    }
    try {
      this.snaps = new SnapStore(path.join(cfg.dir, "snap"));
      if (disk.base > 0) {
        const s = this.snaps.get(disk.base);
        if (!s) throw new StartError(`log base ${disk.base} needs a snapshot that cannot be read`);
        const err = sm.restore(disk.base, s.data);
        if (err !== undefined) throw new StartError(`state machine: ${err}`);
      }
    } catch (e) {
      this.wal.close();
      throw e instanceof StartError ? e : new StartError((e as Error).message);
    }
    this.smApplied = disk.base;
    const seed = (BigInt(Date.now()) * 1000003n + BigInt(process.hrtime()[1])) ^ BigInt(cfg.clusterId) ^ (BigInt(cfg.id) << 32n);
    this.node = new Node(cfg.id, cfg.members.map((m) => m.id), cfg.consensus, seed & ((1n << 64n) - 1n), disk);
  }

  /// Recover from disk and start. Rejects with StartError when the state on disk cannot be trusted.
  static async start(cfg: ReplicaConfig, sm: StateMachine, filter = new Filter()): Promise<Replica> {
    const r = new Replica(cfg, sm, filter);
    try {
      r.transport = new Transport(cfg.id, cfg.members, cfg.clusterId, (i) => r.onInbound(i), r.snaps, filter, cfg.authKey);
      await r.transport.start();
    } catch (e) {
      r.wal.close();
      throw new StartError(`transport: ${(e as Error).message}`);
    }
    r.ticker = setInterval(() => r.onTick(), cfg.tickMs);
    return r;
  }

  get config(): ReplicaConfig { return this.cfg; }

  // ---- public API ----

  /// Submit a pre-encoded log entry payload; resolves once it is committed.
  propose(payload: Uint8Array): Promise<ProposeResult> {
    if (this.stopped || this.halted) return Promise.resolve(res("stopped"));
    const behind = this.node.appliedIndex - this.smApplied;
    if (this.pending.size >= this.cfg.maxPending || behind > this.cfg.maxApplyBacklog) return Promise.resolve(res("busy"));
    const idx = this.node.propose(payload);
    if (idx < 0) return Promise.resolve(res("notleader", 0, this.node.leader));
    return new Promise((resolve) => {
      this.pending.set(idx, { term: this.node.term, resolve, deadline: Date.now() + this.cfg.proposeTimeoutMs });
      this.replicateWanted = true;
      this.kick();
    });
  }

  /// Linearizable read barrier: commits an empty entry and waits for the
  /// state machine to apply it. A deposed leader cannot commit, so it cannot answer.
  async barrier(): Promise<ProposeResult> {
    const r = await this.propose(new Uint8Array(0));
    if (r.kind !== "ok") return r;
    if (this.smApplied < r.index) {
      const ok = await Promise.race([
        new Promise<boolean>((resolve) => this.waiters.push({ index: r.index, resolve: () => resolve(true) })),
        new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 30000).unref()),
      ]);
      if (!ok) return res("timeout");
    }
    return r;
  }

  status(): Status {
    return {
      id: this.cfg.id, role: this.node.role, term: this.node.term, leader: this.node.leader, commit: this.node.commitIndex,
      applied: this.smApplied, base: this.node.base, lastIndex: this.node.lastIndex, pending: this.pending.size,
      halted: this.halted, digestsVerified: this.digestsVerified,
    };
  }

  /// (applied index, state digest), if the state machine supports digests.
  stateHash(): Promise<{ index: number; digest: Digest } | undefined> {
    if (this.stopped || this.halted) return Promise.resolve(undefined);
    return new Promise((resolve) => {
      this.applyQ.push({ kind: "hash", resolve });
      this.scheduleApply();
    });
  }

  /// Graceful stop: queued WAL writes are flushed first.
  stop(): Promise<void> { return this.shutdown(false); }
  /// Simulated power cut: writes not yet fsynced are discarded.
  crash(): Promise<void> { return this.shutdown(true); }

  private async shutdown(abort: boolean): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.aborted = abort;
    if (this.ticker) clearInterval(this.ticker);
    if (this.walBusy) await new Promise<void>((r) => (this.walIdle = r));
    if (!abort && !this.halted && this.walQ.length > 0) {
      try {
        for (const p of this.walQ.splice(0)) this.wal.append(p);
        this.wal.sync(this.cfg.fsync);
      } catch { /* the process is stopping anyway */ }
    }
    this.finish();
    await this.transport.shutdown();
  }

  /// The driver is done: fail what is still waiting.
  private finish(): void {
    for (const p of this.pending.values()) p.resolve(res("unknown"));
    this.pending.clear();
    for (const w of this.waiters.splice(0)) w.resolve();
    for (let i = this.applyHead; i < this.applyQ.length; i++) {
      const j = this.applyQ[i];
      if (j.kind === "hash") j.resolve(undefined);
    }
    this.applyQ = [];
    this.applyHead = 0;
    if (this.walBusy) this.closeWal = true;
    else this.wal.close();
  }

  private get live(): boolean { return !this.stopped && !this.halted; }

  // ---- the driver ----

  private kick(): void {
    if (this.driveScheduled || !this.live) return;
    this.driveScheduled = true;
    setImmediate(() => {
      this.driveScheduled = false;
      this.drive();
    });
  }

  private onInbound(i: Inbound): void {
    if (!this.live) return;
    if (i.kind === "msg") this.node.step(i.from, i.msg);
    else if (i.kind === "snapshot") {
      process.stderr.write(`orderer-cluster node ${this.cfg.id}: installing snapshot ${i.index} from node ${i.from}\n`);
      this.node.installSnapshot(i.from, i.term, i.index, i.lastTerm);
    } else {
      let m = this.digests.get(i.index);
      if (!m) this.digests.set(i.index, (m = new Map()));
      m.set(i.from, i.digest);
      this.checkDigests(i.index);
    }
    this.kick();
  }

  private onTick(): void {
    if (!this.live) return;
    this.node.tick();
    const now = Date.now();
    for (const [i, p] of this.pending) {
      if (p.deadline <= now) {
        p.resolve(res("timeout"));
        this.pending.delete(i);
      }
    }
    this.drive();
  }

  private drive(): void {
    if (!this.live) return;
    if (this.replicateWanted) {
      this.replicateWanted = false;
      this.node.replicate();
    }
    this.pump();
    this.maybeCompact();
    this.pump();
    this.startWal();
    this.scheduleApply();
  }

  /// Execute the protocol core's effects.
  private pump(): void {
    for (;;) {
      const effects = this.node.takeEffects();
      if (effects.length === 0) return;
      for (const e of effects) {
        switch (e.kind) {
          case "send": this.transport.send(e.to, e.msg); break;
          case "persist":
            if (e.persist.compact) this.compactTokens.set(e.persist.token, e.persist.compact.index);
            this.walQ.push(e.persist);
            break;
          case "apply": this.committed(e.index, e.entry); break;
          case "needsnap": this.transport.sendSnapshot(e.to, this.node.term, e.index, e.term); break;
          case "restore": this.applyQ.push({ kind: "restore", index: e.index }); break;
        }
      }
    }
  }

  /// An entry committed: acknowledge its proposer, then queue it for the state machine.
  private committed(index: number, entry: Entry): void {
    const p = this.pending.get(index);
    if (p) {
      p.resolve(p.term === entry.term ? res("ok", index) : res("superseded"));
      this.pending.delete(index);
    }
    this.applyQ.push({ kind: "apply", index, entry });
  }

  // ---- WAL ----

  private startWal(): void {
    if (this.walBusy || this.walQ.length === 0 || !this.live) return;
    const batch = this.walQ.splice(0);
    const token = batch[batch.length - 1].token;
    let compacted = false;
    try {
      for (const p of batch) {
        this.wal.append(p);
        compacted = compacted || p.compact !== null;
      }
      this.wal.flush();
    } catch (e) {
      this.halt(`wal write failed: ${(e as Error).message}`);
      return;
    }
    if (!this.cfg.fsync) {
      this.walDone(token, compacted);
      return;
    }
    this.walBusy = true;
    this.wal.syncAsync((err) => {
      this.walBusy = false;
      if (this.walIdle) { this.walIdle(); this.walIdle = undefined; }
      if (this.closeWal) { this.wal.close(); return; }
      if (this.aborted || !this.live) return;
      if (err) this.halt(`wal write failed: fsync: ${err.message}`);
      else this.walDone(token, compacted);
    });
  }

  private walDone(token: number, compacted: boolean): void {
    try {
      if (compacted) this.wal.rewrite();
    } catch (e) {
      this.halt(`wal write failed: ${(e as Error).message}`);
      return;
    }
    this.node.persisted(token);
    for (const [t, from] of this.compactTokens) {
      if (t <= token) {
        this.snaps.gc(from);
        this.compactTokens.delete(t);
      }
    }
    this.drive();
  }

  // ---- applying ----

  private scheduleApply(): void {
    if (this.applyScheduled || this.applyHead >= this.applyQ.length || !this.live) return;
    this.applyScheduled = true;
    setImmediate(() => {
      this.applyScheduled = false;
      this.applySlice();
    });
  }

  private applySlice(): void {
    if (!this.live) return;
    const t0 = performance.now();
    let stalled = false;
    try {
      while (this.applyHead < this.applyQ.length && this.live) {
        const j = this.applyQ[this.applyHead];
        if (j.kind === "apply") {
          if (this.sm.apply(j.index, j.entry) === false) { stalled = true; break; } // retry the same entry later
          this.smApplied = j.index;
          if (this.cfg.digestEvery > 0 && j.index % this.cfg.digestEvery === 0) {
            const d = this.sm.digest(j.index);
            if (d) {
              let m = this.digests.get(j.index);
              if (!m) this.digests.set(j.index, (m = new Map()));
              m.set(this.cfg.id, d);
              for (const mem of this.cfg.members) if (mem.id !== this.cfg.id) this.transport.sendDigest(mem.id, j.index, d);
              this.checkDigests(j.index);
            }
          }
        } else if (j.kind === "restore") {
          const s = this.snaps.get(j.index);
          if (!s) { this.halt(`reading snapshot ${j.index}`); return; }
          const err = this.sm.restore(j.index, s.data);
          if (err !== undefined) { this.halt(`restoring snapshot: ${err}`); return; }
          this.smApplied = j.index;
        } else {
          const d = deepDigestOf(this.sm, this.smApplied);
          j.resolve(d ? { index: this.smApplied, digest: d } : undefined);
        }
        this.applyQ[this.applyHead++] = undefined as never;
        if (performance.now() - t0 >= APPLY_SLICE_MS) break;
      }
    } catch (e) {
      this.halt(`state machine failed: ${(e as Error).message}`);
      return;
    }
    if (this.applyHead > 4096 && this.applyHead * 2 > this.applyQ.length) {
      this.applyQ = this.applyQ.slice(this.applyHead);
      this.applyHead = 0;
    } else if (this.applyHead === this.applyQ.length) {
      this.applyQ = [];
      this.applyHead = 0;
    }
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      if (this.waiters[i].index <= this.smApplied) this.waiters.splice(i, 1)[0].resolve();
    }
    if (!this.live) return;
    if (stalled) {
      // Give the state machine's downstream a moment before the next try.
      this.applyScheduled = true;
      setTimeout(() => { this.applyScheduled = false; this.applySlice(); }, 1);
    } else {
      this.scheduleApply();
      if (this.cfg.compactEvery > 0) this.kick(); // a drained queue may let a snapshot proceed
    }
  }

  // ---- compaction ----

  /// Snapshot and compact once the state machine has caught up with the log.
  private maybeCompact(): void {
    if (this.cfg.compactEvery === 0 || !this.live) return;
    const applied = this.node.appliedIndex;
    if (this.smApplied !== applied || this.applyHead < this.applyQ.length) return; // snapshot the state at `applied`, not before
    if (applied < this.node.base || applied - this.node.base < this.cfg.compactEvery) return;
    const e = this.node.entryAt(applied);
    if (!e) return;
    try {
      this.snaps.put(applied, e.term, this.sm.snapshot(applied));
    } catch (err) {
      this.halt(`writing snapshot ${applied}: ${(err as Error).message}`);
      return;
    }
    if (applied > this.node.base && applied <= this.node.appliedIndex) this.node.compact(applied);
  }

  // ---- divergence detection ----

  /// Fail-stop if this node's state differs from the majority's.
  private checkDigests(index: number): void {
    const seen = this.digests.get(index);
    const own = seen?.get(this.cfg.id);
    if (!seen || !own) return;
    const n = this.cfg.members.length, quorum = Math.floor(n / 2) + 1;
    const groups = new Map<string, number>();
    for (const d of seen.values()) groups.set(d.toString("hex"), (groups.get(d.toString("hex")) ?? 0) + 1);
    const ownHex = own.toString("hex");
    let otherMajority = false;
    for (const [d, c] of groups) if (c >= quorum && d !== ownHex) otherMajority = true;
    if ((groups.get(ownHex) ?? 0) >= quorum) {
      if (seen.size === n) {
        this.digestsVerified++;
        this.digests.delete(index);
      }
    } else if (otherMajority) {
      this.halt(`state diverged from the majority at index ${index} (own digest ${ownHex})`);
    } else if (seen.size === n) {
      this.halt(`state digests at index ${index} have no majority`);
    }
    if (this.cfg.digestEvery > 0) {
      const span = this.cfg.digestEvery * 64;
      const keepFrom = index > span ? index - span : 0;
      for (const i of this.digests.keys()) if (i < keepFrom) this.digests.delete(i);
    }
  }

  private halt(why: string): void {
    if (this.halted) return;
    process.stderr.write(`orderer-cluster node ${this.cfg.id}: HALT: ${why}\n`);
    this.halted = why;
    if (this.ticker) clearInterval(this.ticker);
    this.finish();
    void this.transport.shutdown();
  }
}

export { sha256, WalError };
