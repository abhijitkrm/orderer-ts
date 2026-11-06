// Scale-out: several independent replicated groups, each owning a disjoint set
// of symbols (spec/REPLICATION.md §9, decision D1; port of orderer-rust's
// shard.rs). One replicated log is bounded by what its leader can replicate;
// sharding multiplies that bound: every group has its own leader, log,
// followers and fsync stream, so groups share nothing and fail independently.
// What it costs is the single global ingress order: the commands of one
// symbol are totally ordered (they all go to one group), but there is no
// order between symbols in different groups.

import { CmdSym, encodeBatch } from "./codec";
import { Client, ClientResult } from "./client";

const M = (1n << 64n) - 1n;

/// Which group owns a symbol. The mix is deliberately different from the
/// pipeline's own symbol-to-partition hash, so the symbols of one group still
/// spread evenly over that group's partitions.
export class ShardMap {
  constructor(readonly groups: number) {
    if (!Number.isInteger(groups) || groups < 1) throw new Error("at least one group");
  }

  /// splitmix64 finalizer, the same as the Rust, C++ and Go maps.
  groupOf(sym: number): number {
    let z = (BigInt(sym) + 0x9e3779b97f4a7c15n) & M;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & M;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & M;
    z ^= z >> 31n;
    return Number(z % BigInt(this.groups));
  }

  /// One sub-batch per group; each symbol's commands keep their relative order.
  split(cmds: ReadonlyArray<CmdSym>): CmdSym[][] {
    const out: CmdSym[][] = Array.from({ length: this.groups }, () => []);
    for (const c of cmds) out[this.groupOf(c[0])].push(c);
    return out;
  }
}

export interface ShardOutcome { sent: boolean; result: ClientResult }

/// One leader-following Client per group.
export class ShardedClient {
  readonly map: ShardMap;
  constructor(private readonly groups: Client[]) {
    this.map = new ShardMap(groups.length);
  }

  /// Submit a batch split by group and report each group's outcome. A failure
  /// in one group does not undo another's commit: the caller sees exactly
  /// which sub-batches are committed.
  async propose(cmds: ReadonlyArray<CmdSym>): Promise<ShardOutcome[]> {
    const parts = this.map.split(cmds);
    return Promise.all(parts.map(async (part, g): Promise<ShardOutcome> => {
      if (part.length === 0) return { sent: false, result: { kind: "ok", index: 0, text: "" } };
      return { sent: true, result: await this.groups[g].propose(encodeBatch(part)) };
    }));
  }

  close(): void { for (const g of this.groups) g.close(); }
}
