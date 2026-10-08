//! Recovery (spec/JOURNAL.md §4–5): restore a snapshot into per-partition
//! cores under the restoring pipeline's routing, then replay command journals
//! merged by iseq.

import * as fs from "fs";
import { BookConfig } from "./matcher/book";
import { Event } from "./matcher/types";
import { CoreFactory, MatchingCore, parseSnapshot } from "./core";
import { get, sameBook, u64 } from "./flat";
import { JournalFormat, mergeJournals, readCmdDir } from "./journal";
import { Initial, Snapshot, metaPath } from "./pipeline";
import { PartitionMap } from "./routing";

export class RecoverError extends Error {}

/// A snapshot body + its .meta sidecar (missing sidecar ⇒ cut 0, e.g. a matcher snapshot).
export function readSnapshot(p: string): Snapshot {
  let body: string;
  try {
    body = fs.readFileSync(p, "utf8");
  } catch {
    throw new RecoverError(`snapshot: ${p}: cannot read`);
  }
  const mp = metaPath(p);
  if (!fs.existsSync(mp)) return new Snapshot(body, 0, 1);
  const line = fs.readFileSync(mp, "utf8").split("\n")[0];
  const iseq = u64(line, "iseq");
  if (get(line, "format") !== "orderer-meta/1" || iseq === undefined) throw new RecoverError(`snapshot: ${mp}: bad sidecar`);
  return new Snapshot(body, iseq, u64(line, "partitions") ?? 1);
}

/// Empty cores, or cores restored from `snap` (its header overrides `book`).
export function restore(core: CoreFactory, book: BookConfig, map: PartitionMap, snap?: Snapshot): { book: BookConfig; cores: MatchingCore[] } {
  if (snap === undefined) return { book, cores: Array.from({ length: map.partitions }, () => core(book)) };
  let ps;
  try {
    ps = parseSnapshot(snap.body);
  } catch (e) {
    throw new RecoverError(`snapshot: ${(e as Error).message}`);
  }
  const cores = Array.from({ length: map.partitions }, () => core(ps.cfg));
  const seen = new Set<number>();
  for (const b of ps.books) {
    if (seen.has(b.symbol)) throw new RecoverError(`snapshot: book ${b.symbol} appears twice`);
    seen.add(b.symbol);
    const e = cores[map.partition(b.symbol)].restoreBook(b.symbol, b.seq, b.orders);
    if (e !== undefined) throw new RecoverError(`snapshot: ${e}`);
  }
  return { book: ps.cfg, cores };
}

export interface JournalSource {
  dir: string;
  format: JournalFormat;
}

export class Recovery {
  constructor(
    readonly book: BookConfig,
    readonly cores: MatchingCore[],
    readonly snapshotIseq: number,
    readonly lastIseq: number,
    readonly replayed: number,
    private readonly snap?: Snapshot,
    private readonly journal?: JournalSource,
  ) {}
  /// A pipeline's starting state: its engines rebuild these books in their workers.
  initial(): Initial {
    return {
      snapshot: this.snap === undefined ? undefined : { body: this.snap.body, iseq: this.snap.iseq },
      journal: this.journal,
      nextIseq: this.lastIseq + 1,
    };
  }
}

/// Snapshot (optional) + every command journal in the source (optional),
/// records after the cut replayed in iseq order. The book config comes from
/// the snapshot, else the journals, else `book`.
export function recover(core: CoreFactory, book: BookConfig, map: PartitionMap, snap: Snapshot | undefined,
  journal: JournalSource | undefined, emit: (p: number, sym: number, seq: number, ev: Event) => void): Recovery {
  const journals = journal === undefined ? undefined : readCmdDir(journal.dir, journal.format);
  if (journals !== undefined && snap === undefined) book = journals.header.book;
  const r = restore(core, book, map, snap);
  if (journals !== undefined && !sameBook(journals.header.book, r.book))
    throw new RecoverError("snapshot: snapshot and journal book configs differ");
  const cut = snap?.iseq ?? 0;
  const recs = journals === undefined ? [] : mergeJournals(journals.partitions, cut);
  const last = recs.length === 0 ? cut : Math.max(cut, recs[recs.length - 1].iseq);
  for (const rec of recs) {
    const p = map.partition(rec.sym);
    r.cores[p].apply(rec.sym, rec.cmd, (s, seq, ev) => emit(p, s, seq, ev));
  }
  return new Recovery(r.book, r.cores, cut, last, recs.length, snap, journal);
}
