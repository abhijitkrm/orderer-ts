// Worker for tests/pipeline.ts: publishes through a Handle built from a
// descriptor, then records completion (and Ok count) in shared memory.
//   mode "stream": publish `cmds`, mixing single and batched publishes
//   mode "race":   publish cancels until Closed, counting Ok publishes live
import { workerData } from "worker_threads";
import { Handle, HandleDescriptor, Status } from "../src/handle";
import { Command } from "../src/matcher/types";

const d = workerData as {
  desc: HandleDescriptor; mode: string; cmds?: Array<[number, Command]>; sym?: number;
  shared: SharedArrayBuffer; index: number; n: number;
};
const s = new Int32Array(d.shared);
const h = new Handle(d.desc);
if (d.mode === "stream") {
  const cmds = d.cmds!;
  for (let i = 0; i < cmds.length; i += 7) {
    const k = Math.min(7, cmds.length - i);
    if (k % 2 === 0) h.publishBatch(cmds, i, i + k);
    else for (let j = 0; j < k; j++) h.publish(cmds[i + j][0], cmds[i + j][1]);
  }
} else {
  for (let i = 0; h.publish(d.sym!, { kind: "cancel", orderId: i }) === Status.Ok; i++) Atomics.add(s, d.n + d.index, 1);
}
h.close();
Atomics.store(s, d.index, 1);
