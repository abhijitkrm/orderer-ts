//! Shared plumbing for the spec/HARNESS.md tools (tools/*.ts).

import * as fs from "fs";
import { BookConfig } from "./matcher/book";
import { Command } from "./matcher/types";
import { acks, collect } from "./egress";
import { get, parseCommand, parseHeader, parseU64, u64 } from "./flat";
import { JournalConfig, fsyncEveryN, fsyncNever, journalConfig } from "./journal";
import { Pipeline, Snapshot, Status } from "./pipeline";
import { PartitionMap } from "./routing";

/// spec/HARNESS.md §5: usage / input / config / corruption errors exit 2.
export function die(msg: string): never {
  process.stderr.write(msg + "\n");
  process.exit(2);
}
/// Internal failures exit 1.
export function fail(msg: string): never {
  process.stderr.write(msg + "\n");
  process.exit(1);
}

export interface Corpus {
  book: BookConfig;
  engine: boolean;
  cmds: Array<[number, Command]>;
}

export function readText(p: string): string {
  try {
    return fs.readFileSync(p, "utf8");
  } catch {
    return die(`${p}: cannot read`);
  }
}

/// A command file (spec/HARNESS.md §1), strictly; throws with the message.
export function parseCorpus(text: string, p: string): Corpus {
  const lines = text.split("\n");
  if (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
  return parseCorpusLines(lines, p);
}

/// Lines of a file too large for one JS string (V8 caps strings near 512 MB).
function bufferLines(b: Buffer): string[] {
  const out: string[] = [];
  for (let pos = 0; pos < b.length; ) {
    let e = b.indexOf(10, pos);
    if (e < 0) e = b.length;
    out.push(b.toString("utf8", pos, e));
    pos = e + 1;
  }
  return out;
}

function parseCorpusLines(lines: string[], p: string): Corpus {
  if (lines.length === 0) throw new Error(`${p}: empty file`);
  const hdr = lines[0].replace(/\r$/, "");
  const c: Corpus = { book: parseHeader(hdr), engine: get(hdr, "engine") === "true", cmds: [] };
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].replace(/\r$/, "");
    if (line.trim() === "") continue;
    const cmd = parseCommand(line);
    if (cmd === undefined) throw new Error(`${p}:${i + 1}: malformed command: ${line}`);
    let sym = 0;
    if (c.engine) {
      const s = u64(line, "symbol");
      if (s === undefined || s > 0xffffffff) throw new Error(`${p}:${i + 1}: missing symbol: ${line}`);
      sym = s;
    }
    c.cmds.push([sym, cmd]);
  }
  return c;
}

export function loadCorpus(p: string): Corpus {
  let b: Buffer;
  try {
    b = fs.readFileSync(p);
  } catch {
    return die(`${p}: cannot read`);
  }
  try {
    return parseCorpusLines(bufferLines(b), p);
  } catch (e) {
    return die((e as Error).message);
  }
}

/// Minimal argv parser: positionals plus --flag / --opt value.
export class Args {
  readonly positional: string[] = [];
  private readonly opts = new Map<string, string>();
  private readonly flags = new Set<string>();
  constructor(argv: string[], usage: string, valued: string[], flags: string[]) {
    for (let i = 0; i < argv.length; i++) {
      const a = argv[i];
      if (!a.startsWith("--")) this.positional.push(a);
      else if (valued.includes(a)) {
        if (i + 1 >= argv.length) die(`${a} needs a value\nusage: ${usage}`);
        this.opts.set(a, argv[++i]);
      } else if (flags.includes(a)) this.flags.add(a);
      else die(`unknown option ${a}\nusage: ${usage}`);
    }
  }
  get(k: string): string | undefined {
    return this.opts.get(k);
  }
  flag(k: string): boolean {
    return this.flags.has(k);
  }
  num(k: string, def: number, max: number): number {
    const v = this.opts.get(k);
    if (v === undefined) return def;
    const n = parseU64(v);
    if (n === undefined || n > max) die(`${k}: not a number: ${v}`);
    return n;
  }
}

export const COMMON_VALUED = ["--partitions", "--partition-map", "--journal-dir"];
export const COMMON_FLAGS = ["--binary"];

export function partitionMap(a: Args): PartitionMap {
  const p = a.num("--partitions", 1, 0xffffffff);
  const mp = a.get("--partition-map");
  try {
    if (mp !== undefined) {
      const text = readText(mp);
      try {
        return PartitionMap.parseTable(text, p);
      } catch (e) {
        return die(`${mp}: ${(e as Error).message}`);
      }
    }
    return PartitionMap.make(p);
  } catch (e) {
    return die((e as Error).message);
  }
}

export interface Common {
  map: PartitionMap;
  journal?: JournalConfig;
}

export function common(a: Args): Common {
  const dir = a.get("--journal-dir");
  if (a.flag("--binary") && dir === undefined) die("--binary requires --journal-dir");
  const c: Common = { map: partitionMap(a) };
  if (dir !== undefined) {
    c.journal = journalConfig(dir, a.flag("--binary") ? "binary" : "jsonl");
    c.journal.fsync = fsyncNever(); // harness runs need complete files, not power-loss safety
  }
  return c;
}

/// spec/HARNESS.md §4.1 options beyond the common ones (1.2).
export interface RunOpts {
  snapshot?: boolean;
  checkpointEvery?: number; // --checkpoint-every K
  durable?: boolean; // --durable
}

/// Run a corpus through a fresh pipeline (one producer, file order), drain,
/// optionally snapshot, shut down. Returns the spec/HARNESS.md §3 listing.
export function runCorpus(corpus: Corpus, c: Common, tagged: boolean, opt: boolean | RunOpts): { listing: string; snap?: Snapshot } {
  const opts: RunOpts = typeof opt === "boolean" ? { snapshot: opt } : opt;
  const snapshot = opts.snapshot === true;
  const [f, events] = collect(tagged);
  const b = Pipeline.builder().bookConfig(corpus.book).partitionMap(c.map).egress(f);
  if (c.journal !== undefined) {
    const j = { ...c.journal };
    if (opts.durable === true) {
      j.fsync = fsyncEveryN(64);
      const last = new Map<number, number>();
      b.egress(acks((p, m) => {
        if (last.get(p) !== m.iseq) {
          last.set(p, m.iseq);
          fs.writeSync(2, `acked ${p} ${m.iseq}\n`);
        }
      }));
    }
    b.journal(j);
  } else if (opts.durable === true || opts.checkpointEvery !== undefined) {
    die("--durable and --checkpoint-every need --journal-dir");
  }
  let p: Pipeline;
  try {
    p = b.build();
  } catch (e) {
    return die((e as Error).message);
  }
  let snap: Snapshot | undefined;
  try {
    const k = opts.checkpointEvery;
    if (k !== undefined) {
      if (k < 1) die("--checkpoint-every: K must be at least 1");
      for (let off = 0; off < corpus.cmds.length; off += k) {
        const n = Math.min(k, corpus.cmds.length - off);
        if (p.publishBatch(corpus.cmds, off, off + n) !== Status.Ok) fail("pipeline closed");
        if (n === k) p.checkpoint();
      }
    } else if (p.publishBatch(corpus.cmds) !== Status.Ok) fail("pipeline closed");
    p.drain();
    if (snapshot) snap = p.snapshot();
    p.shutdown();
  } catch (e) {
    return fail((e as Error).message);
  }
  return { listing: events.listing(), snap };
}

/// Write to stdout (flushed before the process exits), quietly on a closed pipe.
export function print(s: string): void {
  process.stdout.on("error", () => process.exit(0));
  process.stdout.write(s);
}
