//! Symbol → partition (spec/ROUTING.md).

import { get, u64 } from "./flat";

export const MAX_PARTITIONS = 1024;

/// spec/ROUTING.md §2: ((sym * 0x9E3779B97F4A7C15 mod 2^64) >> 32) * P >> 32,
/// exact in doubles: the product's high word is sym*K_hi + floor(sym*K_lo / 2^32)
/// (mod 2^32), with sym split into 16-bit halves so every partial stays below 2^53.
export function hashPartition(sym: number, partitions: number): number {
  const s = sym >>> 0;
  const kLo = 0x7f4a7c15, kHi = 0x9e3779b9;
  const a = (s & 0xffff) * kLo, b = (s >>> 16) * kLo;
  const carry = Math.floor((b + Math.floor(a / 65536)) / 65536);
  const hi = (Math.imul(s, kHi) + carry) >>> 0;
  return Math.floor((hi * partitions) / 4294967296);
}

export class PartitionMap {
  private readonly dense: Int32Array;
  private readonly sparse = new Map<number, number>();
  private constructor(readonly partitions: number) {
    this.dense = new Int32Array(4096);
    for (let s = 0; s < 4096; s++) this.dense[s] = hashPartition(s, partitions);
  }

  /// A fixed map: table overrides, hash for the rest.
  static make(partitions: number, table: Array<[number, number]> = []): PartitionMap {
    if (!Number.isInteger(partitions) || partitions < 1 || partitions > MAX_PARTITIONS)
      throw new Error(`partitions must be 1..=${MAX_PARTITIONS}, got ${partitions}`);
    const m = new PartitionMap(partitions);
    for (const [sym, part] of table) {
      if (part >= partitions) throw new Error(`symbol ${sym}: partition ${part} out of range for ${partitions} partitions`);
      if (m.sparse.has(sym)) throw new Error(`symbol ${sym} listed twice`);
      if (sym < 4096) m.dense[sym] = part;
      m.sparse.set(sym, part);
    }
    return m;
  }

  /// Parse an orderer-partition-map/1 file for a pipeline of `partitions`.
  static parseTable(text: string, partitions: number): PartitionMap {
    const table: Array<[number, number]> = [];
    let first = true;
    for (const raw of text.split("\n")) {
      const line = raw.trim();
      if (line === "") continue;
      if (first) {
        first = false;
        if (get(line, "format") !== "orderer-partition-map/1") throw new Error("not an orderer-partition-map/1 header");
        const pp = u64(line, "partitions");
        if (pp === undefined) throw new Error("partition map header lacks partitions");
        if (pp !== partitions) throw new Error(`partition map is for ${pp} partitions, pipeline has ${partitions}`);
        continue;
      }
      const s = u64(line, "symbol"), q = u64(line, "partition");
      if (s === undefined || s > 0xffffffff) throw new Error(`bad symbol in: ${line}`);
      if (q === undefined || q > 0xffffffff) throw new Error(`bad partition in: ${line}`);
      table.push([s, q]);
    }
    if (first) throw new Error("empty partition map");
    return PartitionMap.make(partitions, table);
  }

  partition(sym: number): number {
    if (sym < 4096) return this.dense[sym];
    if (this.sparse.size > 0) {
      const p = this.sparse.get(sym);
      if (p !== undefined) return p;
    }
    return hashPartition(sym, this.partitions);
  }

  /// The table overrides (to rebuild the map in a worker).
  table(): Array<[number, number]> {
    return [...this.sparse.entries()];
  }
}
