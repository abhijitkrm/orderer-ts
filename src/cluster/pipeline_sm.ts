// The orderer pipeline as the replicated state machine (port of orderer-rust's
// pipeline_sm.rs). A committed log entry carries a batch of commands; applying
// it publishes the batch into the local Pipeline in order, so every replica's
// partitions see the same ingress order and emit byte-identical events
// (spec/PIPELINE.md §3). The pipeline runs without journaling: the replicated
// log is the journal.
//
// Divergence detection uses an event-chain digest (spec/REPLICATION.md 13.16): a rolling hash
// over the events each partition emits, so `digest` costs a pipeline barrier, not a snapshot of
// every book. `deepDigest` is the full-state hash, SHA-256 of `iseq u64 | partitions u32 |
// matcher-snap/1 body`, computed on demand. Snapshots carry the chains: `PSM2 | iseq u64 |
// partitions u32 | chain u64 per partition | body`. All bytes match the Rust, C++, Go and Java nodes.

import { Entry } from "../consensus";
import { eventDigest, EventDigestHandle, metrics } from "../egress";
import { Pipeline, Status } from "../pipeline";
import { BookConfig, defaultConfig } from "../matcher/book";
import { CmdSym, CodecError, decodeBatch, Out } from "./codec";
import { Digest, sha256, StateMachine } from "./statemachine";

const SNAP_MAGIC = Buffer.from("PSM2", "ascii");
const DIGEST_TAG = Buffer.from("orderer-evdigest/1", "ascii");

export class PipelineSm implements StateMachine {
  private pipeline: Pipeline;
  private chains!: EventDigestHandle;
  /// Commands applied so far (the pipeline's iseq).
  private iseq = 0;
  /// The entry being published, when the ingress ring filled part-way through it.
  private cur?: { index: number; cmds: CmdSym[]; at: number };
  badEntries = 0;

  /// A pipeline with a counting (non-accumulating) egress.
  constructor(private readonly partitions: number, private readonly book: BookConfig = defaultConfig()) {
    this.pipeline = this.build();
  }

  private build(init?: { body: string; iseq: number }, chains: bigint[] = []): Pipeline {
    const [egress] = metrics(0);
    const [digest, handle] = eventDigest(this.partitions, chains);
    const b = Pipeline.builder().bookConfig(this.book).partitions(this.partitions).egress(egress).egress(digest).timestamps(false);
    if (init) b.initial({ snapshot: init, nextIseq: init.iseq + 1 });
    const p = b.build();
    this.chains = handle;
    return p;
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
    this.iseq += cur.cmds.length;
    this.cur = undefined;
  }

  /// The snapshot: the full-state encoding plus the event chains, so a restored replica continues them.
  private encodeSnapshot(): Buffer {
    this.pipeline.drain(); // egress has seen everything before the snapshot is taken
    const s = this.pipeline.snapshot();
    const chains = this.chains.values();
    const o = new Out(64 + 8 * chains.length + s.body.length).bytes(SNAP_MAGIC).u64(s.iseq).u32(s.partitions);
    for (const c of chains) o.u64big(c);
    return o.bytes(Buffer.from(s.body, "utf8")).done();
  }

  snapshot(): Uint8Array { return this.encodeSnapshot(); }

  restore(_index: number, d: Uint8Array): string | undefined {
    const head = 16 + 8 * this.partitions;
    const b = Buffer.from(d.buffer, d.byteOffset, d.length);
    if (b.length < head || !b.subarray(0, 4).equals(SNAP_MAGIC)) return "not a pipeline snapshot (PSM2)";
    const iseq = b.readUInt32LE(4) + b.readUInt32LE(8) * 4294967296;
    const partitions = b.readUInt32LE(12);
    if (partitions !== this.partitions) return `snapshot has ${partitions} partitions, this replica runs ${this.partitions}`;
    const chains: bigint[] = [];
    for (let i = 0; i < partitions; i++) chains.push(b.readBigUInt64LE(16 + 8 * i));
    this.cur = undefined;
    const old = this.chains;
    try {
      const next = this.build({ body: b.toString("utf8", head), iseq }, chains);
      this.pipeline.shutdown();
      this.pipeline = next;
      this.iseq = iseq;
    } catch (e) {
      this.chains = old;
      return (e as Error).message;
    }
    return undefined;
  }

  /// The event-chain digest: O(partitions) after one pipeline barrier.
  digest(): Digest {
    this.pipeline.drain();
    const o = new Out(64).bytes(DIGEST_TAG).u64(this.iseq).u32(this.partitions);
    for (const c of this.chains.values()) o.u64big(c);
    return sha256(o.done());
  }

  /// The full-state hash: SHA-256 of `iseq | partitions | matcher-snap/1 body`. Costs a snapshot of every book.
  deepDigest(): Digest {
    this.pipeline.drain();
    const s = this.pipeline.snapshot();
    return sha256(new Out(64 + s.body.length).u64(s.iseq).u32(s.partitions).bytes(Buffer.from(s.body, "utf8")).done());
  }
}
