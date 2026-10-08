// Ring protocol tests (orderer-rust tests/ring.rs, ported): wrap,
// multi-producer integrity across workers, batching, gating, try-publish CAS
// path, barrier dependencies, wait strategies, multicast.
import * as path from "path";
import { Worker } from "worker_threads";
import { Consumer, MultiProducer, Publish, SingleProducer, backoff, blocking, busySpin, createRing, yieldWait } from "../src/ring";
import { check, eq, runAll } from "./t";

const PRODUCER = path.join(__dirname, "ring_producer.js");

function drainN(c: Consumer, n: number, f: (v: DataView, o: number, seq: number, eob: boolean) => void): void {
  const deadline = performance.now() + 20_000;
  for (let got = 0; got < n; ) {
    got += c.poll(f);
    if (performance.now() > deadline) return check(false, `timed out at ${got}`);
  }
}

function spscWrap(): void {
  const r = createRing(8, 8, "single", [[]]);
  const p = new SingleProducer(r), c = new Consumer(r, 0);
  let expect = 0, bad = 0;
  for (let i = 0; i < 100_000; i++) {
    p.publish((v, o) => v.setFloat64(o, i, true));
    if ((i & 7) === 7) c.poll((v, o, seq) => { if (v.getFloat64(o, true) !== expect || seq !== expect) bad++; expect++; });
  }
  check(bad === 0 && expect === 100_000, `bad ${bad}, consumed ${expect}`);
}

function multiProducerRun(batch: number): void {
  const PRODUCERS = 4, PER = 50_000;
  const r = createRing(1024, 24, "multi", [[]]);
  const c = new Consumer(r, 0);
  const accepted = new SharedArrayBuffer(8);
  const ws = Array.from({ length: PRODUCERS }, (_, id) => new Worker(PRODUCER, { workerData: { ring: r, id, per: PER, batch, accepted } }));
  const next = new Array(PRODUCERS).fill(0);
  let last = -1, bad = 0;
  drainN(c, PRODUCERS * PER, (v, o, seq) => {
    const id = v.getFloat64(o, true), k = v.getFloat64(o + 8, true);
    if (seq !== last + 1 || v.getFloat64(o + 16, true) !== id * 31 + k * 7 || k !== next[id]) bad++;
    last = seq;
    next[id]++;
  });
  for (const w of ws) void w.terminate();
  check(bad === 0, `${bad} ordering / torn-slot violations`);
}

function tryProducersNeverDoubleClaim(): void {
  const PER = 20_000;
  const r = createRing(64, 24, "multi", [[]]);
  const c = new Consumer(r, 0);
  const accepted = new SharedArrayBuffer(8); // [accepted, producers done]
  const acc = new Int32Array(accepted);
  const ws = Array.from({ length: 4 }, (_, id) => new Worker(PRODUCER, { workerData: { ring: r, id, per: PER, batch: id % 2 === 0 ? 1 : 0, accepted } }));
  const seen = new Set<string>();
  let last = -1, bad = 0;
  const deadline = performance.now() + 30_000;
  while (performance.now() < deadline) {
    const n = c.poll((v, o, seq) => {
      const key = `${v.getFloat64(o, true)}:${v.getFloat64(o + 8, true)}`;
      if (seq !== last + 1 || seen.has(key)) bad++;
      last = seq;
      seen.add(key);
    });
    if (n === 0 && Atomics.load(acc, 1) === 4 && seen.size === Atomics.load(acc, 0)) break;
  }
  for (const w of ws) void w.terminate();
  check(bad === 0, "delivered twice / out of order");
  check(seen.size === Atomics.load(acc, 0), `${seen.size} delivered vs ${Atomics.load(acc, 0)} accepted`);
}

function batchClaimConsumeAndStaging(): void {
  const r = createRing(16, 8, "single", [[]]);
  const p = new SingleProducer(r), c = new Consumer(r, 0);
  p.publishBatch(5, (i, v, o) => v.setFloat64(o, i * 10, true));
  const eobs: boolean[] = [];
  check(c.poll((_v, _o, _s, e) => eobs.push(e)) === 5);
  check(eobs[4] && !eobs[0], "end_of_batch on the last only");
  p.view.setFloat64(p.stage(), 1, true);
  p.view.setFloat64(p.stage(), 2, true);
  check(p.staged() === 2);
  check(c.poll(() => {}) === 0, "staged is invisible");
  p.commit();
  check(c.poll(() => {}) === 2);
  p.publishBatch(10, (i, v, o) => v.setFloat64(o, i, true));
  c.setMaxBatch(4);
  check(c.poll(() => {}) === 4);
}

function gatingAndTryPublishFull(): void {
  const r = createRing(4, 8, "single", [[]]);
  const p = new SingleProducer(r), c = new Consumer(r, 0);
  for (let i = 0; i < 4; i++) check(p.tryPublish((v, o) => v.setFloat64(o, i, true)) === Publish.Ok);
  check(p.tryPublish(() => {}) === Publish.Full);
  const seen: number[] = [];
  c.poll((v, o) => seen.push(v.getFloat64(o, true)));
  check(p.tryPublish((v, o) => v.setFloat64(o, 4, true)) === Publish.Ok);
  c.poll((v, o) => seen.push(v.getFloat64(o, true)));
  check(eq(seen, [0, 1, 2, 3, 4]), `zero loss: ${seen}`);
}

function tryPublishCasNeverLeaksClaims(): void {
  const r = createRing(8, 8, "multi", [[]]);
  const p = new MultiProducer(r), c = new Consumer(r, 0);
  for (let i = 0; i < 8; i++) p.tryPublish((v, o) => v.setFloat64(o, i, true));
  check(p.tryPublish(() => {}) === Publish.Full && p.tryPublishBatch(3, () => {}) === Publish.Full);
  check(p.published() === 7, "a failed try leaves the cursor untouched");
  check(c.poll(() => {}) === 8);
  check(p.tryPublish(() => {}) === Publish.Ok);
}

function barrierDependencyOrdersStages(): void {
  const r = createRing(256, 8, "single", [[], [0]]);
  const p = new SingleProducer(r), a = new Consumer(r, 0), b = new Consumer(r, 1);
  let bad = 0, gotB = 0;
  for (let i = 0; i < 50_000; i++) {
    p.publish((v, o) => v.setFloat64(o, i, true));
    if (i % 3 === 0) gotB += b.poll((v, o, seq) => { if (v.getFloat64(o, true) !== seq || a.sequence() < seq) bad++; });
    if (i % 2 === 0) a.poll(() => {});
  }
  a.poll(() => {});
  gotB += b.poll((v, o, seq) => { if (a.sequence() < seq) bad++; });
  check(bad === 0 && gotB === 50_000, `stage B overtook stage A (${bad}), consumed ${gotB}`);
}

function waitStrategiesAllDeliver(): void {
  for (const w of [busySpin(), yieldWait(), backoff(), blocking()]) {
    const r = createRing(16, 8, "single", [[]]);
    const p = new SingleProducer(r), c = new Consumer(r, 0, w);
    const seen: number[] = [];
    for (let i = 0; i < 20; i++) {
      p.publish((v, o) => v.setFloat64(o, i, true));
      check(c.waitPoll((v, o) => seen.push(v.getFloat64(o, true))));
    }
    p.alert();
    check(!c.waitPoll(() => {}), "alerted and empty");
    check(seen.length === 20 && seen[19] === 19, w.kind);
  }
}

function multicastSlowestGates(): void {
  const r = createRing(8, 8, "single", [[], [], []]);
  const p = new SingleProducer(r);
  const cs = [0, 1, 2].map((i) => new Consumer(r, i));
  for (let i = 0; i < 8; i++) p.publish(() => {});
  check(cs[0].poll(() => {}) === 8 && cs[1].poll(() => {}) === 8);
  check(p.tryPublish(() => {}) === Publish.Full, "third consumer gates");
  check(cs[2].poll(() => {}) === 8 && p.tryPublish(() => {}) === Publish.Ok);
}

runAll([
  ["spsc_wrap", spscWrap],
  ["multi_producer_integrity_single_claims", () => multiProducerRun(1)],
  ["multi_producer_integrity_batched_claims", () => multiProducerRun(37)],
  ["try_and_block_producers_never_double_claim", tryProducersNeverDoubleClaim],
  ["batch_claim_consume_and_staging", batchClaimConsumeAndStaging],
  ["gating_and_try_publish_full", gatingAndTryPublishFull],
  ["try_publish_cas_never_leaks_claims", tryPublishCasNeverLeaksClaims],
  ["barrier_dependency_orders_stages", barrierDependencyOrdersStages],
  ["wait_strategies_all_deliver", waitStrategiesAllDeliver],
  ["multicast_all_see_everything_slowest_gates", multicastSlowestGates],
]);
