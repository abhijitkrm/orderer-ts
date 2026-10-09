// Worker for orderbench --producers N: waits on a shared start flag, then
// publishes its stream through a Handle in batches and records completion.
// ctl words: [0] ready count, [1] start flag, [2] done count.
import { workerData } from "worker_threads";
import { Handle, HandleDescriptor } from "../src/handle";
import { Command } from "../src/matcher/types";

const d = workerData as { desc: HandleDescriptor; cmds: Array<[number, Command]>; batch: number; ctl: SharedArrayBuffer };
const ctl = new Int32Array(d.ctl);
const h = new Handle(d.desc);
Atomics.add(ctl, 0, 1);
while (Atomics.load(ctl, 1) === 0) Atomics.wait(ctl, 1, 0, 50);
for (let i = 0; i < d.cmds.length; i += d.batch) h.publishBatch(d.cmds, i, Math.min(i + d.batch, d.cmds.length));
h.close();
Atomics.add(ctl, 2, 1);
