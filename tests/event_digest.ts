// The event-chain digest (spec/REPLICATION.md 13.16): the chain step against a BigInt reference,
// conformance vectors shared with the other languages, sensitivity, continuity across a snapshot
// restore, cost, and the documented limit.
//
//   node dist/tests/event_digest.js
import * as fs from "fs";
import * as path from "path";
import { eventDigest, chainStepBig } from "../src/egress";
import { loadCorpus } from "../src/harness";
import { Pipeline } from "../src/pipeline";
import { BookConfig } from "../src/matcher/book";
import { Command, Side, Tif, newLimit } from "../src/matcher/types";
import { CmdSym, DedupSm, PipelineSm, encodeBatch } from "../src/cluster";
import { sha256 } from "../src/cluster/statemachine";
import { Out } from "../src/cluster/codec";
import { check, runAllAsync } from "./t";
import { testBook } from "./cluster_util";

const VEC = path.join(__dirname, "..", "..", "vectors");
const TAG = Buffer.from("orderer-evdigest/1", "ascii");

/// The replica-level digest over tag, commands applied, partitions and chains.
function combine(iseq: number, chains: bigint[]): Buffer {
  const o = new Out(64).bytes(TAG).u64(iseq).u32(chains.length);
  for (const c of chains) o.u64big(c);
  return sha256(o.done());
}

/// Run `cmds` through a fresh pipeline and return its chains.
function chains(book: BookConfig, cmds: ReadonlyArray<CmdSym>, partitions: number): bigint[] {
  const [digest, handle] = eventDigest(partitions);
  const p = Pipeline.builder().bookConfig(book).partitions(partitions).egress(digest).build();
  try {
    p.publishBatch(cmds);
    p.drain();
    return handle.values();
  } finally {
    p.shutdown();
  }
}

const M64 = (1n << 64n) - 1n;
const refStep = (h: bigint, x: bigint): bigint => {
  const m = ((h ^ x) * 0x9e3779b97f4a7c15n) & M64;
  return ((m << 31n) | (m >> 33n)) & M64;
};

const entry = (cmds: CmdSym[]) => ({ term: 1, data: encodeBatch(cmds) });
/// Apply one entry, yielding to the event loop while the ring is full: the pipeline's egress is
/// pumped from the loop, exactly as the replica services the network between retries.
async function applyAll(sm: PipelineSm, index: number, cmds: CmdSym[]): Promise<void> {
  while (sm.apply(index, entry(cmds)) === false) await new Promise((r) => setTimeout(r, 1));
}

runAllAsync([
  ["the_chain_step_matches_a_bigint_reference", async () => {
    let x = 0x2545f4914f6cdd1dn;
    const next = () => ((x = (x ^ (x << 13n)) & M64), (x ^= x >> 7n), (x = (x ^ (x << 17n)) & M64), x);
    for (let i = 0; i < 20000; i++) {
      const h = next(), v = i % 3 === 0 ? next() >> 12n : next();
      check(chainStepBig(h, v) === refStep(h, v), `step(${h}, ${v})`);
    }
    // Edges: zero, all ones, carries across the 32-bit halves.
    for (const h of [0n, M64, 0xffffffffn, 0x100000000n, 0x8000000000000000n])
      for (const v of [0n, M64, 1n, 0xffffffffn, 0x100000000n]) check(chainStepBig(h, v) === refStep(h, v), `edge ${h} ${v}`);
  }],

  ["chains_match_the_shared_vectors", async () => {
    let n = 0;
    for (const name of ["multisymbol", "fuzz_s11"]) {
      const c = loadCorpus(path.join(VEC, "pipeline", `${name}.cmd.jsonl`));
      for (const p of [1, 2, 4]) {
        const got = chains(c.book, c.cmds, p);
        const lines = fs.readFileSync(path.join(VEC, "replication-digest", `${name}.P${p}.txt`), "utf8").trim().split("\n");
        const want = lines[3].split(" ").slice(1);
        check(lines[1] === `partitions ${p}` && lines[2] === `commands ${c.cmds.length}`, `${name} P${p} header`);
        check(got.map((v) => v.toString(16).padStart(16, "0")).join(" ") === want.join(" "), `${name} P${p} chains`);
        check(combine(c.cmds.length, got).toString("hex") === lines[4].split(" ")[1], `${name} P${p} digest`);
        n++;
      }
    }
    check(n === 6, `${n} vector files`);
  }],

  ["chains_are_deterministic_and_sensitive_to_every_field", async () => {
    const base: CmdSym[] = [];
    for (let i = 1; i <= 40; i++) base.push([i % 5, newLimit(i, i % 2 === 0 ? Side.Bid : Side.Ask, 100 + (i % 3), 1 + (i % 4), Tif.Gtc)]);
    // Sweeping orders trade against whatever rests, which exposes resting prices and quantities.
    for (const s of [0, 1, 2, 3, 4, 7, 8, 9, 10, 11]) {
      base.push([s, newLimit(9000 + s, Side.Bid, 1000, 10000, Tif.Ioc)]);
      base.push([s, newLimit(9100 + s, Side.Ask, 0, 10000, Tif.Ioc)]);
    }
    const book = testBook();
    const reference = chains(book, base, 2);
    check(reference.join() === chains(book, base, 2).join(), "not deterministic");
    const tweak = (f: (c: CmdSym) => CmdSym) => chains(book, [f(base[0]), ...base.slice(1)], 2).join();
    const edit = (c: CmdSym, patch: Partial<Extract<Command, { kind: "new" }>>): CmdSym => [c[0], { ...(c[1] as Extract<Command, { kind: "new" }>), ...patch }];
    const c0 = base[0][1] as Extract<Command, { kind: "new" }>;
    check(reference.join() !== tweak((c) => edit(c, { qty: c0.qty + 1 })), "qty");
    check(reference.join() !== tweak((c) => edit(c, { price: c0.price + 1 })), "price");
    check(reference.join() !== tweak((c) => edit(c, { orderId: c0.orderId + 1000 })), "order id");
    check(reference.join() !== tweak((c) => [c[0] + 7, c[1]]), "symbol");
    check(reference.join() !== chains(book, base.slice(0, 39), 2).join(), "dropped command");
  }],

  ["a_restored_pipeline_continues_the_same_chain", async () => {
    const c = loadCorpus(path.join(VEC, "pipeline", "fuzz_s11.cmd.jsonl"));
    const P = 2, whole = chains(c.book, c.cmds, P);
    const [d1, h1] = eventDigest(P);
    const first = Pipeline.builder().bookConfig(c.book).partitions(P).egress(d1).build();
    let snap, mid: bigint[];
    try {
      first.publishBatch(c.cmds, 0, 1500);
      first.drain();
      snap = first.snapshot();
      mid = h1.values();
    } finally {
      first.shutdown();
    }
    const [d2, h2] = eventDigest(P, mid);
    const second = Pipeline.builder().bookConfig(c.book).partitions(P).egress(d2)
      .initial({ snapshot: { body: snap.body, iseq: snap.iseq }, nextIseq: snap.iseq + 1 }).build();
    try {
      second.publishBatch(c.cmds, 1500);
      second.drain();
      check(h2.values().join() === whole.join(), "the chain broke across a snapshot");
    } finally {
      second.shutdown();
    }
  }],

  ["the_digest_costs_a_barrier_not_a_snapshot", async () => {
    const sm = new PipelineSm(2, testBook());
    try {
      // 300k resting orders over 8 symbols: bids and asks that never cross.
      for (let from = 1; from <= 300_000; from += 10_000) {
        const cmds: CmdSym[] = [];
        for (let i = from; i < from + 10_000; i++)
          cmds.push([i % 8, i % 2 === 0 ? newLimit(i, Side.Bid, 1000 - (i % 500), 1, Tif.Gtc) : newLimit(i, Side.Ask, 2000 + (i % 500), 1, Tif.Gtc)]);
        await applyAll(sm, 1, cmds);
      }
      let t = performance.now();
      const cheap = sm.digest();
      const cheapMs = performance.now() - t;
      t = performance.now();
      const deep = sm.deepDigest();
      const deepMs = performance.now() - t;
      console.log(`  digest ${cheapMs.toFixed(1)} ms, deep digest ${deepMs.toFixed(1)} ms (300k resting orders)`);
      check(!cheap.equals(deep), "the two digests are the same value");
      check(cheapMs < deepMs / 10, `digest ${cheapMs} ms is not much cheaper than deep ${deepMs} ms`);
      check(cheapMs < 100, `digest took ${cheapMs} ms`);
    } finally {
      sm.close();
    }
  }],

  ["a_resting_difference_is_invisible_to_the_chain_until_it_trades", async () => {
    // The limit of an event-stream digest, stated as a test: books that differ only in a resting
    // order's price emit the same events until something trades against it. The deep digest
    // sees it at once.
    const run = async (price: number) => {
      const sm = new PipelineSm(1, testBook());
      try {
        await applyAll(sm, 1, [[1, newLimit(1, Side.Ask, price, 5, Tif.Gtc)]]);
        return [sm.digest(), sm.deepDigest()];
      } finally {
        sm.close();
      }
    };
    const [chainA, deepA] = await run(100), [chainB, deepB] = await run(101);
    check(chainA.equals(chainB), "the chain saw a price that has not traded");
    check(!deepA.equals(deepB), "the deep digest missed a different resting price");
  }],

  ["snapshots_carry_the_chains_and_refuse_foreign_layouts", async () => {
    const a = new PipelineSm(2, testBook()), b = new PipelineSm(2, testBook()), other = new PipelineSm(1, testBook());
    try {
      const cmds: CmdSym[] = [];
      for (let i = 1; i <= 200; i++) cmds.push([i % 4, newLimit(i, i % 2 ? Side.Bid : Side.Ask, 100 + (i % 3), 2, Tif.Gtc)]);
      await applyAll(a, 1, cmds);
      const snap = a.snapshot();
      check(Buffer.from(snap.subarray(0, 4)).toString() === "PSM2", "magic");
      check(b.restore(1, snap) === undefined, "restore failed");
      check(a.digest().equals(b.digest()) && a.deepDigest().equals(b.deepDigest()), "restored replica differs");
      // Both keep going and stay equal.
      const more: CmdSym[] = [[1, newLimit(9001, Side.Bid, 105, 50, Tif.Ioc)], [2, newLimit(9002, Side.Ask, 90, 50, Tif.Ioc)]];
      await applyAll(a, 2, more);
      await applyAll(b, 2, more);
      check(a.digest().equals(b.digest()), "chains diverged after the restore");
      check(typeof other.restore(1, snap) === "string", "a snapshot with another partition count was accepted");
      check(typeof b.restore(1, Buffer.from("PSM1xxxxxxxxxxxxxxxxxxxxxxxxxxxx")) === "string", "a foreign layout was accepted");
      // The session wrapper forwards both digests.
      const d = new DedupSm(a);
      check(d.digest(1) !== undefined && d.deepDigest(1) !== undefined && !d.digest(1)!.equals(d.deepDigest(1)!), "dedup forwarding");
    } finally {
      a.close(); b.close(); other.close();
    }
  }],
]);
