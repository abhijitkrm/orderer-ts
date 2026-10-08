// Worker for tests/ring.ts: attaches a MultiProducer and publishes
// [producer, counter, check] triples, `batch` per claim (0: tryPublish loop).
import { workerData } from "worker_threads";
import { MultiProducer, Publish, RingShared } from "../src/ring";

const { ring, id, per, batch, accepted } = workerData as { ring: RingShared; id: number; per: number; batch: number; accepted: SharedArrayBuffer };
const p = new MultiProducer(ring);
const acc = new Int32Array(accepted);
const fill = (v: DataView, o: number, k: number) => {
  v.setFloat64(o, id, true);
  v.setFloat64(o + 8, k, true);
  v.setFloat64(o + 16, id * 31 + k * 7, true);
};
if (batch === 0) {
  for (let i = 0; i < per; i++) if (p.tryPublish((v, o) => fill(v, o, i)) === Publish.Ok) Atomics.add(acc, 0, 1);
} else {
  for (let i = 0; i < per; ) {
    const n = Math.min(batch, per - i), base = i;
    p.publishBatch(n, (k, v, o) => fill(v, o, base + k));
    Atomics.add(acc, 0, n);
    i += n;
  }
}
Atomics.add(acc, 1, 1); // this producer is done
