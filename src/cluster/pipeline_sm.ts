// The orderer pipeline as the replicated state machine (port of orderer-rust's
// pipeline_sm.rs). A committed log entry carries a batch of commands; applying
// it publishes the batch into the local Pipeline in order, so every replica's
// partitions see the same ingress order and emit byte-identical events
// (spec/PIPELINE.md §3). The pipeline runs without journaling: the replicated
// log is the journal. The state serialised for snapshots and digests is
// `iseq u64 | partitions u32 | matcher-snap/1 body`, the same bytes the Rust,
// C++ and Go nodes produce, so replicas of every language agree on digests.

import { Entry } from "../consensus";
import { metrics } from "../egress";
import { Pipeline, Status } from "../pipeline";
import { BookConfig, defaultConfig } from "../matcher/book";
import { CmdSym, CodecError, decodeBatch, Out } from "./codec";
import { Digest, sha256, StateMachine } from "./statemachine";

export class PipelineSm implements StateMachine {
  private pipeline: Pipeline;
  /// The entry being published, when the ingress ring filled part-way through it.
  private cur?: { index: number; cmds: CmdSym[]; at: number };
  badEntries = 0;

  /// A pipeline with a counting (non-accumulating) egress.
  constructor(private readonly partitions: number, private readonly book: BookConfig = defaultConfig()) {
    this.pipeline = this.build();
  }

  private build(init?: { body: string; iseq: number }): Pipeline {
    const [egress] = metrics(0);
    const b = Pipeline.builder().bookConfig(this.book).partitions(this.partitions).egress(egress).timestamps(false);
    if (init) b.initial({ snapshot: init, nextIseq: init.iseq + 1 });
    return b.build();
  }

  close(): void {
    this.pipeline.shutdown();
  }

  /// Publishes without ever blocking: a full ingress ring (the engines are
  /// busy, e.g. creating a symbol's first book) returns false and the replica
  /// calls again after servicing the network, so heartbeats keep flowing.
  apply(index: number, e: Entry): void | false {
    let cur = this.cur;
    if (!cur || cur.index !== index) {
      let cmds;
      try {
        cmds = decodeBatch(e.data);
      } catch (err) {
        if (!(err instanceof CodecError)) throw err;
        this.badEntries++; // every TS replica skips it identically
        return;
      }
      if (cmds.length === 0) return;
      cur = this.cur = { index, cmds, at: 0 };
    }
    while (cur.at < cur.cmds.length) {
      const [sym, cmd] = cur.cmds[cur.at];
      const st = this.pipeline.tryPublish(sym, cmd);
      if (st === Status.Full) return false;
      if (st !== Status.Ok) throw new Error("pipeline rejected committed commands");
      cur.at++;
    }
    this.cur = undefined;
  }

  private encodeState(): Buffer {
    this.pipeline.drain(); // egress has seen everything before the snapshot is taken
    const s = this.pipeline.snapshot();
    return new Out(64 + s.body.length).u64(s.iseq).u32(s.partitions).bytes(Buffer.from(s.body, "utf8")).done();
  }

  snapshot(): Uint8Array { return this.encodeState(); }

  restore(_index: number, d: Uint8Array): string | undefined {
    if (d.length < 12) return "short pipeline snapshot";
    const b = Buffer.from(d.buffer, d.byteOffset, d.length);
    const iseq = b.readUInt32LE(0) + b.readUInt32LE(4) * 4294967296;
    this.cur = undefined;
    try {
      const next = this.build({ body: b.toString("utf8", 12), iseq });
      this.pipeline.shutdown();
      this.pipeline = next;
    } catch (e) {
      return (e as Error).message;
    }
    return undefined;
  }

  digest(): Digest { return sha256(this.encodeState()); }
}
