// End to end: real replicas over TCP with real files (mirrors orderer-go's
// cluster tests, orderer-rust's orderer-cluster/tests and orderer-cpp's
// cluster_e2e_test), plus the matching pipeline as the replicated state machine.
//
//   node dist/tests/cluster.js
import * as fs from "fs";
import * as net from "net";
import * as path from "path";
import { NONE } from "../src/consensus";
import { Client, ClientServer, Conn, DedupSm, RecorderSm, Replica, decodeBatch, encodeBatch } from "../src/cluster";
import { newLimit, Side, Tif } from "../src/matcher/types";
import { check, runAllAsync } from "./t";
import { ClusterOpts, TestCluster, expectSeq, freeAddrs, idOf, payload, seq, sleep, until } from "./cluster_util";
import type { CmdSym } from "../src/cluster";

const ids3 = [0, 1, 2];
const other = (l: number) => (l === 0 ? 1 : 0);

async function withCluster(n: number, o: ClusterOpts, body: (c: TestCluster) => Promise<void>): Promise<void> {
  const c = await TestCluster.create(n, o ?? {});
  try {
    await body(c);
  } finally {
    await c.close();
  }
}

const batch = (id: number): CmdSym[] => [[1, newLimit(id, Side.Bid, 10000, 1, Tif.Gtc)]];

const tests: Array<[string, () => Promise<void>]> = [
  ["elects a leader and replicates in order", () => withCluster(3, {}, async (c) => {
    await c.waitLeader();
    for (let i = 0; i < 500; i++) await c.mustPropose(payload(i));
    expectSeq(await c.waitConverged(), 0, 500, "replicated log");
  })],

  ["a single node cluster works", () => withCluster(1, {}, async (c) => {
    await c.waitLeader();
    for (let i = 0; i < 100; i++) await c.mustPropose(payload(i));
    check((await c.waitConverged(10000)).length === 100, "entries");
  })],

  ["followers refuse proposals and point to the leader", () => withCluster(3, {}, async (c) => {
    const l = await c.waitLeader();
    await c.mustPropose(payload(0));
    await c.waitApplied([0, 1, 2], 1);
    const r = await c.nodes[other(l)]!.propose(payload(1));
    check(r.kind === "notleader" && r.hint === l, "no redirect: ", JSON.stringify(r), " leader ", l);
  })],

  ["leader crash loses no acknowledged command", () => withCluster(3, {}, async (c) => {
    await c.waitLeader();
    const acked = new Set<number>(), maybe = new Set<number>();
    const run = async (a: number, b: number) => {
      for (let i = a; i < b; i++) ((await c.propose(payload(i))).kind === "ok" ? acked : maybe).add(i);
    };
    await run(0, 200);
    const old = c.leader();
    await c.crash(old);
    await run(200, 600);
    await c.restart(old);
    const got = await c.waitConverged(20000);
    const seen = new Set<number>();
    let prev = 0;
    for (const p of got) {
      const id = idOf(p);
      check(!seen.has(id), "command ", id, " applied twice");
      seen.add(id);
      check(id >= prev, "commands reordered");
      prev = id;
    }
    for (const a of acked) check(seen.has(a), "acknowledged command ", a, " lost");
    for (const g of seen) check(acked.has(g) || maybe.has(g), g, " was never proposed");
  })],

  ["an isolated leader cannot commit and its uncommitted entries vanish", () => withCluster(3, { tune: (c) => (c.proposeTimeoutMs = 700) }, async (c) => {
    const old = await c.waitLeader();
    for (let i = 0; i < 20; i++) await c.mustPropose(payload(i));
    await c.waitApplied([0, 1, 2], 20);
    c.nodes[old]!.filter.isolate(old, ids3);
    const r = await c.nodes[old]!.propose(payload(1000));
    check(["timeout", "notleader", "superseded"].includes(r.kind), "an isolated leader must not acknowledge: ", r.kind);
    let newl = -1;
    await until(() => { for (let i = 0; i < 3; i++) if (i !== old && c.nodes[i]!.status().role === "Leader") newl = i; return newl >= 0; }, 10000, "majority did not elect a leader");
    for (let i = 20; i < 60; i++) {
      for (;;) {
        const p = await c.nodes[newl]!.propose(payload(i));
        if (p.kind === "ok") break;
        check(p.kind === "busy" || p.kind === "notleader", "majority side failed: ", p.kind);
        await sleep(5);
      }
    }
    c.nodes[old]!.filter.heal();
    expectSeq(await c.waitConverged(15000), 0, 60, "entry 1000 must not survive");
  })],

  ["a lagging node catches up by snapshot", () => withCluster(3, { tune: (c) => (c.compactEvery = 40) }, async (c) => {
    const l = await c.waitLeader();
    const lag = other(l);
    for (let i = 0; i < 10; i++) await c.mustPropose(payload(i));
    await c.waitApplied([0, 1, 2], 10);
    await c.stop(lag);
    for (let i = 10; i < 400; i++) await c.mustPropose(payload(i));
    await until(() => c.nodes[l]!.status().base > 0, 10000, "leader never compacted");
    await c.restart(lag);
    expectSeq(await c.waitConverged(20000), 0, 400, "after snapshot catch-up");
    check(c.nodes[lag]!.status().base > 0, "caught up without a snapshot install");
  })],

  ["a whole cluster restart recovers from disk", () => withCluster(3, { tune: (c) => { c.compactEvery = 50; c.fsync = true; } }, async (c) => {
    await c.waitLeader();
    for (let i = 0; i < 300; i++) await c.mustPropose(payload(i));
    await c.waitConverged(10000);
    for (let i = 0; i < 3; i++) await c.stop(i);
    for (let i = 0; i < 3; i++) await c.restart(i);
    await c.waitLeader(10000);
    for (let i = 300; i < 400; i++) await c.mustPropose(payload(i));
    expectSeq(await c.waitConverged(15000), 0, 400, "after full restart");
  })],

  ["a torn WAL tail recovers and a corrupt WAL refuses to start", () => withCluster(3, { tune: (c) => (c.fsync = true) }, async (c) => {
    const l = await c.waitLeader();
    for (let i = 0; i < 100; i++) await c.mustPropose(payload(i));
    await c.waitConverged(10000);
    const f = other(l);
    await c.crash(f);
    const wal = path.join(c.dir, `n${f}`, "wal", "raft.wal");
    fs.appendFileSync(wal, Buffer.from([0x40, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3])); // half a frame, as a torn write leaves
    await c.restart(f);
    for (let i = 100; i < 150; i++) await c.mustPropose(payload(i));
    expectSeq(await c.waitConverged(10000), 0, 150, "after torn-tail recovery");
    // Now real damage: flip a byte early in the file, with valid frames after it.
    await c.stop(f);
    const data = fs.readFileSync(wal);
    data[40] ^= 0xff;
    fs.writeFileSync(wal, data);
    let refused = false;
    try {
      const r = await Replica.start(c.config(f), new RecorderSm());
      await r.crash();
    } catch {
      refused = true;
    }
    check(refused, "a node with mid-log damage must not join");
  })],

  ["state divergence halts the odd node out", () => withCluster(3, { tune: (c) => (c.digestEvery = 16) }, async (c) => {
    await c.stop(2);
    c.shared[2] = new (c.shared[0].constructor as typeof import("./cluster_util").Shared)();
    c.shared[2].corruptAt = 5;
    await c.start(2);
    await c.waitLeader();
    // If the corrupted node happens to lead, it halts itself mid-run and
    // proposals in flight to it have no definite answer; the majority carries on.
    let acked = 0;
    for (let i = 0; i < 200; i++) if ((await c.propose(payload(i))).kind === "ok") acked++;
    check(acked >= 150, "only ", acked, " of 200 acknowledged");
    let why = "";
    await until(() => (why = c.nodes[2]!.status().halted) !== "", 10000, "divergence went undetected");
    check(why.includes("diverged"), "divergence went undetected: '", why, "'");
    check(c.nodes[0]!.status().halted === "" && c.nodes[1]!.status().halted === "", "a healthy node halted");
  })],

  ["matching state passes digest checks", () => withCluster(3, { tune: (c) => (c.digestEvery = 16) }, async (c) => {
    await c.waitLeader();
    for (let i = 0; i < 200; i++) await c.mustPropose(payload(i));
    await c.waitConverged(10000);
    await until(() => c.nodes[0]!.status().digestsVerified > 0, 5000, "no digest was ever verified");
    for (let i = 0; i < 3; i++) check(c.nodes[i]!.status().halted === "", "node ", i, " halted");
  })],

  ["hostile connections do not disturb the cluster", () => withCluster(3, {}, async (c) => {
    await c.waitLeader();
    const [host, port] = [c.members[0].addr.split(":")[0], Number(c.members[0].addr.split(":")[1])];
    const junk = [Buffer.alloc(64, 0xff), Buffer.from([0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0, 1]),
      Buffer.from([10, 0, 0, 0, 0, 0, 0, 0, 6, 9, 1, 2, 3, 4, 5, 6, 7, 8]), Buffer.alloc(0)];
    for (const j of junk) {
      await new Promise<void>((res) => {
        const s = net.connect({ host, port }, () => { if (j.length) s.end(j); else s.end(); });
        s.on("error", () => res());
        s.on("close", () => res());
      });
    }
    for (let i = 0; i < 100; i++) await c.mustPropose(payload(i));
    check((await c.waitConverged(10000)).length === 100, "entries");
  })],

  ["five nodes survive two failures", () => withCluster(5, {}, async (c) => {
    await c.waitLeader();
    for (let i = 0; i < 100; i++) await c.mustPropose(payload(i));
    const l = c.leader();
    await c.crash(l);
    await c.crash(l === 0 ? 1 : 0);
    await c.waitLeader(15000);
    for (let i = 100; i < 200; i++) await c.mustPropose(payload(i));
    expectSeq(await c.waitConverged(15000), 0, 200, "after two failures");
  })],

  ["three nodes cannot commit with two down", () => withCluster(3, { tune: (c) => (c.proposeTimeoutMs = 500) }, async (c) => {
    const l = await c.waitLeader();
    await c.mustPropose(payload(0));
    for (let i = 0; i < 3; i++) if (i !== l) await c.crash(i);
    const r = await c.nodes[l]!.propose(payload(1));
    check(r.kind === "timeout", "no quorum, no ack: ", r.kind);
    check(c.shared[l].payloads().length === 1, "an unacknowledged entry was applied");
  })],

  ["a backlog of slow applies does not cost the leader its leadership", () => withCluster(3, {
    tune: (c) => {
      c.maxPending = 100000;
      c.proposeTimeoutMs = 30000;
      c.consensus.electionTicks = c.id === 0 ? 8 : 40; // node 0 wins the first election
    },
  }, async (c) => {
    // Applies run in time slices on the protocol thread: each of these takes
    // 20 ms, longer than node 0's own election timeout, yet heartbeats keep
    // flowing between applies. (A single apply longer than the election
    // timeout would stall the node: see the deviation note in replica.ts.)
    check((await c.waitLeader()) === 0, "node 0 should lead");
    c.shared[0].applyDelayMs = 20;
    const term0 = c.nodes[0]!.status().term;
    const rs = await Promise.all(Array.from({ length: 30 }, (_, i) => c.nodes[0]!.propose(payload(i))));
    check(rs.every((r) => r.kind === "ok"), "proposals: ", rs.map((r) => r.kind).join());
    const s = c.nodes[0]!.status();
    check(s.role === "Leader" && s.term === term0, "leadership was lost while the state machine was slow");
    await c.waitApplied([1, 2], 30, 5000);
    await c.waitApplied([0], 30, 30000);
  })],

  ["a cluster with one shared key works and a wrong key cannot join", () => withCluster(3, { tune: (c) => (c.authKey = Buffer.from("right")) }, async (c) => {
    const l = await c.waitLeader();
    for (let i = 0; i < 100; i++) await c.mustPropose(payload(i));
    check((await c.waitConverged(10000)).length === 100, "entries");
    const f = other(l);
    await c.stop(f);
    const cfg = c.config(f);
    cfg.authKey = Buffer.from("wrong");
    const rogue = await Replica.start(cfg, new RecorderSm());
    await sleep(1500);
    check(rogue.status().commit === 0, "a node with the wrong key received log entries");
    const term = c.nodes[l]!.status().term;
    for (let i = 100; i < 140; i++) await c.mustPropose(payload(i));
    check(c.nodes[l]!.status().term === term, "an outsider disturbed the cluster");
    await rogue.crash();
  })],

  ["the client protocol, sessions and linearizable reads", () => withCluster(3, { dedup: true }, async (c) => {
    const l = await c.waitLeader();
    const addrs = await freeAddrs(3);
    const servers = await Promise.all(addrs.map((a, i) => ClientServer.listen(a, c.nodes[i]!)));
    try {
      const pairs = addrs.map((addr, id) => ({ id, addr }));
      // Followers first: the client must find the leader by hint.
      const order = [...pairs.filter((_, i) => i !== l), pairs[l]];
      const client = new Client(order).withSession(7);
      for (let i = 1; i <= 20; i++) check((await client.proposeSession(batch(i))).kind === "ok", "session batch ", i);
      // The same session batch committed twice (a retry) takes effect once.
      const dup = encodeBatch(batch(999), { client: 8n, seq: 1n });
      await c.mustPropose(dup);
      await c.mustPropose(dup);
      await c.waitConverged(10000);
      let with999 = 0;
      for (const p of c.shared[0].payloads()) for (const [, cm] of decodeBatch(p)) if (cm.orderId === 999) with999++;
      check(with999 === 1, "a retried batch took effect ", with999, " times");
      // Status, hash and barrier over the wire; a follower cannot serve a barrier.
      const conn = await Conn.connect(addrs[l]);
      check(conn !== undefined, "cannot connect");
      if (conn) {
        for (let i = 0; i < 3; i++) check((await conn.status())?.includes("role=Leader") === true, "status");
        check((await conn.hash()) !== undefined, "hash");
        check((await conn.barrier()) !== undefined, "barrier");
        conn.close();
      }
      const fc = await Conn.connect(addrs[other(l)]);
      check(fc !== undefined && (await fc.barrier()) === undefined, "a follower served a barrier");
      fc?.close();
      // A client without a session cannot use proposeSession.
      const plain = new Client(pairs);
      check((await plain.proposeSession(batch(1))).kind === "protocol", "plain client");
      client.close();
    } finally {
      await Promise.all(servers.map((s) => s.shutdown()));
    }
  })],

  ["a deposed leader cannot serve a stale read", () => withCluster(3, { tune: (c) => (c.proposeTimeoutMs = 600) }, async (c) => {
    const old = await c.waitLeader();
    await c.mustPropose(payload(1));
    await c.waitApplied([0, 1, 2], 1);
    c.nodes[old]!.filter.isolate(old, ids3);
    let newl = -1;
    await until(async () => {
      for (let i = 0; i < 3; i++) if (i !== old && (await c.nodes[i]!.barrier()).kind === "ok") newl = i;
      return newl >= 0;
    }, 10000, "no new leader served a barrier");
    check((await c.nodes[newl]!.propose(payload(2))).kind === "ok" && (await c.nodes[newl]!.barrier()).kind === "ok", "new leader cannot commit");
    check(c.shared[old].count() === 1, "the isolated node applied entries it could not have received");
    check((await c.nodes[old]!.barrier()).kind !== "ok", "a deposed leader served a barrier");
  })],

  // ---- the matching pipeline as the state machine ----

  ["pipeline replicas hold identical state", () => withCluster(3, { pipeline: true, tune: (c) => (c.digestEvery = 32) }, async (c) => {
    await c.waitLeader();
    const rng = new Rng(1);
    let last = { kind: "", index: 0 };
    for (let i = 0; i < 200; i++) {
      last = await c.propose(encodeBatch(rng.batch(20)));
      check(last.kind === "ok", "batch ", i, ": ", last.kind);
    }
    const h = await hashes(c, last.index);
    check(h.every((x) => x === h[0]), "replica state differs");
    await until(() => c.nodes[0]!.status().digestsVerified > 0, 5000, "no digest verified");
    c.nodes.forEach((r, i) => check(r!.status().halted === "", "node ", i, " halted: ", r!.status().halted));
  })],

  ["a lagging pipeline replica catches up from a snapshot of real book state", () => withCluster(3, { pipeline: true, tune: (c) => (c.compactEvery = 40) }, async (c) => {
    const l = await c.waitLeader();
    const lag = other(l);
    const rng = new Rng(2);
    let last = { kind: "", index: 0 };
    const send = async (n: number) => {
      for (let i = 0; i < n; i++) { last = await c.propose(encodeBatch(rng.batch(15))); check(last.kind === "ok", "batch: ", last.kind); }
    };
    await send(10);
    await c.stop(lag);
    await send(300);
    await until(() => c.nodes[l]!.status().base > 0, 10000, "leader never compacted");
    await c.restart(lag);
    await send(5);
    const h = await hashes(c, last.index, 20000);
    check(h.every((x) => x === h[0]), "state differs after snapshot install");
    check(c.nodes[lag]!.status().base > 0, "caught up without a snapshot install");
  })],

  ["pipeline state survives a whole cluster restart from disk", () => withCluster(3, { pipeline: true, tune: (c) => { c.compactEvery = 30; c.fsync = true; } }, async (c) => {
    await c.waitLeader();
    const rng = new Rng(3);
    let last = { kind: "", index: 0 };
    for (let i = 0; i < 150; i++) { last = await c.propose(encodeBatch(rng.batch(10))); check(last.kind === "ok", "batch: ", last.kind); }
    const before = await hashes(c, last.index);
    for (let i = 0; i < 3; i++) await c.stop(i);
    for (let i = 0; i < 3; i++) await c.restart(i);
    await c.waitLeader(10000);
    // The new leader's no-op advances the log by one; compare after it applies.
    const after = await hashes(c, last.index + 1, 10000);
    check(after.every((x) => x === before[0]), "a replica recovered different state");
  })],
];

/// Applied index and digest of every running replica, once each has applied `want`.
async function hashes(c: TestCluster, want: number, ms = 10000): Promise<string[]> {
  let out: string[] = [];
  await until(async () => {
    out = [];
    for (const r of c.nodes) {
      if (!r) continue;
      const h = await r.stateHash();
      if (!h || h.index < want) return false;
      if (h.index !== want) { check(false, "applied ", h.index, " past the expected ", want); return true; }
      out.push(h.digest.toString("hex"));
    }
    return true;
  }, ms, "replicas did not reach the expected index");
  return out;
}

/// Deterministic command batches: limits on both sides that cross, some cancels.
class Rng {
  private s: number;
  private next_ = 0;
  constructor(seed: number) { this.s = seed * 2654435761 >>> 0 || 1; }
  private u(): number { this.s ^= this.s << 13; this.s >>>= 0; this.s ^= this.s >>> 17; this.s ^= this.s << 5; this.s >>>= 0; return this.s; }
  batch(n: number): CmdSym[] {
    const out: CmdSym[] = [];
    for (let i = 0; i < n; i++) {
      const id = ++this.next_;
      const sym = 1 + (this.u() % 8);
      if (this.u() % 10 === 0 && id > 20) { out.push([sym, { kind: "cancel", orderId: id - 1 - (this.u() % 15) }]); continue; }
      out.push([sym, newLimit(id, this.u() % 2 === 0 ? Side.Ask : Side.Bid, 10000 + (this.u() % 9) - 4, 1 + (this.u() % 9), Tif.Gtc)]);
    }
    return out;
  }
}

void seq; void DedupSm; void NONE;
runAllAsync(tests);
