// consensus.ts — the replication protocol core (spec/REPLICATION.md) as a
// pure state machine: no I/O, no clock. The caller feeds inputs (tick, step,
// propose, persisted, compact, installSnapshot) and executes the effects it
// emits. A faithful port of orderer-rust's orderer-consensus; the conformance
// vectors in vectors/replication/ prove the two produce identical effects and
// state for the same inputs.

export type NodeId = number;
export type Term = number;
export type Index = number;

/** "No node" in optional node fields (leader, vote). */
export const NONE = -1;

const M64 = (1n << 64n) - 1n;

/** xorshift64* seeded through splitmix64, bit-identical to the Rust port. */
export class Rng {
  private s: bigint;
  constructor(seed: bigint) {
    let z = (seed + 0x9e3779b97f4a7c15n) & M64;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & M64;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & M64;
    z ^= z >> 31n;
    this.s = z === 0n ? 1n : z;
  }
  next(): bigint {
    let x = this.s;
    x ^= x >> 12n;
    x ^= (x << 25n) & M64;
    x ^= x >> 27n;
    this.s = x;
    return (x * 0x2545f4914f6cdd1dn) & M64;
  }
  below(n: number): number {
    return Number(this.next() % BigInt(n));
  }
}

export interface Entry {
  term: Term;
  data: Uint8Array;
}

export type Msg =
  | { kind: "vote"; term: Term; lastIndex: Index; lastTerm: Term; pre: boolean }
  | { kind: "votereply"; term: Term; granted: boolean; pre: boolean }
  | { kind: "append"; term: Term; prevIndex: Index; prevTerm: Term; entries: Entry[]; commit: Index }
  | { kind: "appendreply"; term: Term; success: boolean; index: Index };

/** One atomic disk write: hard state, optional log-prefix discard, truncate to from-1, append. */
export interface Persist {
  token: number;
  term: Term;
  votedFor: number; // NONE or a node id
  from: Index;
  entries: Entry[];
  compact: { index: Index; term: Term } | null;
}

export type Effect =
  | { kind: "send"; to: NodeId; msg: Msg }
  | { kind: "persist"; persist: Persist }
  | { kind: "apply"; index: Index; entry: Entry }
  | { kind: "needsnap"; to: NodeId; index: Index; term: Term }
  | { kind: "restore"; index: Index; term: Term };

/** What survives a crash. */
export interface Disk {
  term: Term;
  votedFor: number;
  log: Entry[];
  base: Index;
  baseTerm: Term;
}

export interface Config {
  electionTicks: number;
  heartbeatTicks: number;
  maxBatch: number;
  preVote: boolean;
}

export type Role = "Follower" | "PreCandidate" | "Candidate" | "Leader";

interface Write {
  token: number;
  upto: Index;
  epoch: number;
  gated: Effect[];
}

/** One replica's protocol state. */
export class Node {
  private readonly peers: NodeId[];
  private readonly rng: Rng;
  private term_: Term;
  private votedFor: number;
  private log: Entry[];
  private base_: Index;
  private baseTerm: Term;
  private role_: Role = "Follower";
  private leader_: number = NONE;
  private commit_: Index;
  private applied_: Index;
  private durable: Index;
  private epoch = 0;
  private writes: Write[] = [];
  private nextTok = 1;
  private votes: boolean[];
  private heard: boolean[];
  private nextIndex: Index[];
  private matchIndex: Index[];
  private snapWait: number[];
  private electionElapsed = 0;
  private electionTimeout = 0;
  private heartbeatElapsed = 0;
  private out: Effect[] = [];
  fault = "";

  constructor(readonly id: NodeId, peers: NodeId[], private readonly cfg: Config, seed: bigint, disk: Disk) {
    this.peers = [...peers].sort((a, b) => a - b);
    this.rng = new Rng((seed ^ (BigInt(id) << 56n)) & M64);
    this.term_ = disk.term;
    this.votedFor = disk.votedFor;
    this.log = [...disk.log];
    this.base_ = disk.base;
    this.baseTerm = disk.baseTerm;
    this.commit_ = this.applied_ = disk.base;
    this.durable = disk.base + disk.log.length;
    const n = this.peers.length;
    this.votes = new Array(n).fill(false);
    this.heard = new Array(n).fill(false);
    this.nextIndex = new Array(n).fill(1);
    this.matchIndex = new Array(n).fill(0);
    this.snapWait = new Array(n).fill(0);
    this.resetTimeout();
  }

  // ---- observers ----
  get role(): Role { return this.role_; }
  get term(): Term { return this.term_; }
  get leader(): number { return this.leader_; }
  get commitIndex(): Index { return this.commit_; }
  get appliedIndex(): Index { return this.applied_; }
  get base(): Index { return this.base_; }
  get lastIndex(): Index { return this.base_ + this.log.length; }

  takeEffects(): Effect[] {
    const o = this.out;
    this.out = [];
    return o;
  }

  private entryAt(i: Index): Entry | undefined {
    return i > this.base_ && i - this.base_ - 1 < this.log.length ? this.log[i - this.base_ - 1] : undefined;
  }

  // ---- inputs ----
  tick(): void {
    for (let i = 0; i < this.snapWait.length; i++) if (this.snapWait[i] > 0) this.snapWait[i]--;
    if (this.role_ === "Leader") {
      this.heartbeatElapsed++;
      if (this.heartbeatElapsed >= this.cfg.heartbeatTicks) {
        this.heartbeatElapsed = 0;
        this.broadcastAppend();
      }
      this.electionElapsed++;
      if (this.electionElapsed >= this.electionTimeout) {
        // Check-quorum: step down without contact from a majority.
        const me = this.pos(this.id);
        let got = 0;
        for (let i = 0; i < this.heard.length; i++) if (this.heard[i] || i === me) got++;
        if (got < this.quorum()) {
          this.role_ = "Follower";
          this.leader_ = NONE;
        }
        this.heard.fill(false);
        this.resetTimeout();
      }
      return;
    }
    this.electionElapsed++;
    if (this.electionElapsed >= this.electionTimeout) this.startElection();
  }

  /** Appends client data; only the leader accepts. Returns the entry's index, or -1. */
  propose(data: Uint8Array): number {
    if (this.role_ !== "Leader") return -1;
    this.leaderAppend({ term: this.term_, data });
    return this.lastIndex;
  }

  replicate(): void {
    if (this.role_ === "Leader") this.broadcastAppend();
  }

  /** The host stored a snapshot after applying `index`; drop the log prefix it covers. */
  compact(index: Index): void {
    if (index <= this.base_ || index > this.applied_) {
      this.fault = "compact outside (base, applied]";
      return;
    }
    const t = this.termAt(index);
    this.log = this.log.slice(index - this.base_);
    this.base_ = index;
    this.baseTerm = t;
    this.persistWith(this.lastIndex + 1, [], { index, term: t });
  }

  /** The host received and stored the leader's snapshot. */
  installSnapshot(from: NodeId, term: Term, index: Index, lastTerm: Term): void {
    if (term < this.term_) return;
    this.becomeFollower(term, from);
    const replyTerm = this.term_;
    if (index <= this.commit_) {
      this.sendGated(from, { kind: "appendreply", term: replyTerm, success: true, index: this.commit_ });
      return;
    }
    if (index <= this.lastIndex && this.termAt(index) === lastTerm) {
      this.log = this.log.slice(index - this.base_);
    } else {
      this.log = [];
      this.durable = index;
      this.epoch++;
    }
    this.base_ = index;
    this.baseTerm = lastTerm;
    this.commit_ = this.applied_ = index;
    this.persistWith(this.lastIndex + 1, [], { index, term: lastTerm });
    this.out.push({ kind: "restore", index, term: lastTerm });
    this.sendGated(from, { kind: "appendreply", term: replyTerm, success: true, index });
  }

  /** The disk finished the write with `token` (and all earlier ones). */
  persisted(token: number): void {
    while (this.writes.length > 0 && this.writes[0].token <= token) {
      const w = this.writes.shift()!;
      if (w.epoch === this.epoch) this.durable = Math.min(Math.max(this.durable, w.upto), this.lastIndex);
      for (const e of w.gated) this.out.push(e);
    }
    if (this.role_ === "Leader") this.tryCommit();
  }

  step(from: NodeId, m: Msg): void {
    switch (m.kind) {
      case "vote": this.onVote(from, m.term, m.lastIndex, m.lastTerm, m.pre); break;
      case "votereply": this.onVoteReply(from, m.term, m.granted, m.pre); break;
      case "append": this.onAppend(from, m.term, m.prevIndex, m.prevTerm, m.entries, m.commit); break;
      case "appendreply": this.onAppendReply(from, m.term, m.success, m.index); break;
    }
  }

  // ---- helpers ----
  private pos(id: NodeId): number { return this.peers.indexOf(id); }
  private quorum(): number { return Math.floor(this.peers.length / 2) + 1; }
  private lastTerm(): Term { return this.log.length === 0 ? this.baseTerm : this.log[this.log.length - 1].term; }
  private termAt(i: Index): Term {
    if (i === this.base_) return this.baseTerm;
    return this.entryAt(i)?.term ?? 0;
  }
  private resetTimeout(): void {
    this.electionElapsed = 0;
    const t = this.cfg.electionTicks;
    this.electionTimeout = t + this.rng.below(t);
  }
  private others(): NodeId[] { return this.peers.filter((p) => p !== this.id); }
  private send(to: NodeId, msg: Msg): void { this.out.push({ kind: "send", to, msg }); }
  private sendGated(to: NodeId, msg: Msg): void {
    const e: Effect = { kind: "send", to, msg };
    if (this.writes.length > 0) this.writes[this.writes.length - 1].gated.push(e);
    else this.out.push(e);
  }
  private persist(from: Index, entries: Entry[]): void { this.persistWith(from, entries, null); }
  private persistWith(from: Index, entries: Entry[], compact: { index: Index; term: Term } | null): void {
    const token = this.nextTok++;
    this.writes.push({ token, upto: from - 1 + entries.length, epoch: this.epoch, gated: [] });
    this.out.push({ kind: "persist", persist: { token, term: this.term_, votedFor: this.votedFor, from, entries, compact } });
  }
  private persistHardState(): void { this.persist(this.lastIndex + 1, []); }
  private truncateFrom(index: Index): void {
    if (index <= this.commit_) this.fault = "truncating a committed entry";
    this.log = this.log.slice(0, index - this.base_ - 1);
    this.durable = Math.min(this.durable, index - 1);
    this.epoch++;
  }
  private becomeFollower(term: Term, leader: number): void {
    if (term > this.term_) {
      this.term_ = term;
      this.votedFor = NONE;
      this.persistHardState();
    }
    this.role_ = "Follower";
    this.leader_ = leader;
    this.resetTimeout();
  }
  private applyCommitted(): void {
    while (this.applied_ < this.commit_) {
      this.applied_++;
      this.out.push({ kind: "apply", index: this.applied_, entry: this.entryAt(this.applied_)! });
    }
  }

  // ---- elections ----
  private startElection(): void {
    if (this.cfg.preVote && this.peers.length > 1) {
      this.role_ = "PreCandidate";
      this.leader_ = NONE;
      this.votes.fill(false);
      this.votes[this.pos(this.id)] = true;
      this.resetTimeout();
      for (const p of this.others()) {
        this.send(p, { kind: "vote", term: this.term_ + 1, lastIndex: this.lastIndex, lastTerm: this.lastTerm(), pre: true });
      }
    } else {
      this.campaign();
    }
  }
  private voteCount(): number { return this.votes.filter((v) => v).length; }
  private campaign(): void {
    this.term_++;
    this.votedFor = this.id;
    this.role_ = "Candidate";
    this.leader_ = NONE;
    this.votes.fill(false);
    this.votes[this.pos(this.id)] = true;
    this.persistHardState();
    this.resetTimeout();
    for (const p of this.others()) {
      this.sendGated(p, { kind: "vote", term: this.term_, lastIndex: this.lastIndex, lastTerm: this.lastTerm(), pre: false });
    }
    if (this.voteCount() >= this.quorum()) this.becomeLeader();
  }
  private becomeLeader(): void {
    this.role_ = "Leader";
    this.leader_ = this.id;
    const next = this.lastIndex + 1;
    this.nextIndex.fill(next);
    this.matchIndex.fill(0);
    this.heard.fill(false);
    this.heartbeatElapsed = 0;
    this.electionElapsed = 0;
    this.leaderAppend({ term: this.term_, data: new Uint8Array(0) }); // the term's no-op
    this.broadcastAppend();
  }
  private upToDate(lastIndex: Index, lastTerm: Term): boolean {
    return lastTerm !== this.lastTerm() ? lastTerm > this.lastTerm() : lastIndex >= this.lastIndex;
  }
  private onVote(from: NodeId, term: Term, lastIndex: Index, lastTerm: Term, pre: boolean): void {
    const utd = this.upToDate(lastIndex, lastTerm);
    if (pre) {
      const live = this.role_ === "Leader" || (this.leader_ !== NONE && this.electionElapsed < this.electionTimeout);
      const granted = term > this.term_ && utd && !live;
      this.send(from, { kind: "votereply", term: granted ? term : this.term_, granted, pre: true });
      return;
    }
    if (term < this.term_) {
      this.send(from, { kind: "votereply", term: this.term_, granted: false, pre: false });
      return;
    }
    if (term > this.term_) this.becomeFollower(term, NONE);
    const free = this.votedFor === NONE || this.votedFor === from;
    const granted = free && utd;
    if (granted) {
      if (this.votedFor !== from) {
        this.votedFor = from;
        this.persistHardState();
      }
      this.resetTimeout();
    }
    this.sendGated(from, { kind: "votereply", term: this.term_, granted, pre: false });
  }
  private onVoteReply(from: NodeId, term: Term, granted: boolean, pre: boolean): void {
    if (pre) {
      if (this.role_ !== "PreCandidate") return;
      if (!granted) {
        if (term > this.term_) this.becomeFollower(term, NONE);
        return;
      }
      if (term !== this.term_ + 1) return;
      this.votes[this.pos(from)] = true;
      if (this.voteCount() >= this.quorum()) this.campaign();
      return;
    }
    if (term > this.term_) {
      this.becomeFollower(term, NONE);
      return;
    }
    if (this.role_ !== "Candidate" || term < this.term_ || !granted) return;
    this.votes[this.pos(from)] = true;
    if (this.voteCount() >= this.quorum()) this.becomeLeader();
  }

  // ---- replication ----
  private leaderAppend(e: Entry): void {
    this.log.push(e);
    this.persist(this.lastIndex, [e]);
  }
  private broadcastAppend(): void {
    for (const p of this.others()) this.sendAppend(p);
  }
  private sendAppend(to: NodeId): void {
    const i = this.pos(to);
    const next = Math.max(this.nextIndex[i], 1);
    if (next <= this.base_) {
      if (this.snapWait[i] === 0) {
        this.snapWait[i] = this.cfg.electionTicks * 2;
        this.out.push({ kind: "needsnap", to, index: this.base_, term: this.baseTerm });
      }
      return;
    }
    const prev = next - 1;
    const start = prev - this.base_;
    const es = this.log.slice(start, Math.min(this.log.length, start + this.cfg.maxBatch));
    this.nextIndex[i] = next + es.length;
    this.send(to, { kind: "append", term: this.term_, prevIndex: prev, prevTerm: this.termAt(prev), entries: es, commit: this.commit_ });
  }
  private onAppend(from: NodeId, term: Term, prevIndexIn: Index, prevTermIn: Term, entriesIn: Entry[], leaderCommit: Index): void {
    if (term < this.term_) {
      this.send(from, { kind: "appendreply", term: this.term_, success: false, index: 0 });
      return;
    }
    this.becomeFollower(term, from);
    let prevIndex = prevIndexIn, prevTerm = prevTermIn, entries = entriesIn;
    if (prevIndex < this.base_) {
      const skip = Math.min(this.base_ - prevIndex, entries.length);
      entries = entries.slice(skip);
      prevIndex += skip;
      if (prevIndex < this.base_) {
        this.sendGated(from, { kind: "appendreply", term: this.term_, success: true, index: this.base_ });
        return;
      }
      prevTerm = this.baseTerm;
    }
    if (prevIndex > this.lastIndex || this.termAt(prevIndex) !== prevTerm) {
      const hint = Math.min(this.lastIndex, prevIndex > 0 ? prevIndex - 1 : 0);
      this.sendGated(from, { kind: "appendreply", term: this.term_, success: false, index: hint });
      return;
    }
    let idx = prevIndex;
    let firstNew = -1;
    for (let k = 0; k < entries.length; k++) {
      idx++;
      if (idx <= this.lastIndex) {
        if (this.termAt(idx) !== entries[k].term) {
          this.truncateFrom(idx);
          firstNew = k;
          break;
        }
      } else {
        firstNew = k;
        break;
      }
    }
    if (firstNew >= 0) {
      const fresh = entries.slice(firstNew);
      for (const e of fresh) this.log.push(e);
      this.persist(prevIndex + 1 + firstNew, fresh);
    }
    const matched = prevIndex + entries.length;
    if (leaderCommit > this.commit_) {
      this.commit_ = Math.min(leaderCommit, matched);
      this.applyCommitted();
    }
    this.sendGated(from, { kind: "appendreply", term: this.term_, success: true, index: matched });
  }
  private onAppendReply(from: NodeId, term: Term, success: boolean, index: Index): void {
    if (term > this.term_) {
      this.becomeFollower(term, NONE);
      return;
    }
    if (this.role_ !== "Leader" || term < this.term_) return;
    const i = this.pos(from);
    this.heard[i] = true;
    if (success) {
      this.matchIndex[i] = Math.max(this.matchIndex[i], index);
      this.nextIndex[i] = Math.max(this.nextIndex[i], index + 1);
      this.tryCommit();
      if (this.nextIndex[i] <= this.lastIndex) this.sendAppend(from);
    } else {
      this.nextIndex[i] = Math.max(Math.min(this.nextIndex[i] - 1, index + 1), 1);
      this.sendAppend(from);
    }
  }
  private tryCommit(): void {
    const me = this.pos(this.id);
    const m = this.peers.map((_, i) => (i === me ? this.durable : this.matchIndex[i]));
    m.sort((a, b) => b - a);
    const idx = m[this.quorum() - 1];
    // Only current-term entries are committed by counting (Raft figure 8).
    if (idx > this.commit_ && this.termAt(idx) === this.term_) {
      this.commit_ = idx;
      this.applyCommitted();
    }
  }
}
