//! orderrecover — spec/HARNESS.md §4.3 (mirrors matcherrecover).
//!   orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]
//!   orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]
//! Tail form: restore, submit every tail line without "format" through a
//! pipeline, print the replayed events. Journal form: recover from journals
//! (after the optional snapshot's cut; by default the directory's newest
//! checkpoint). Malformed/corrupt input exits 2; --repair first truncates torn
//! tails (spec/JOURNAL.md §5.1).
import { defaultConfig } from "../src/matcher/book";
import { Command, eventCanonical } from "../src/matcher/types";
import { CORES, parseSnapshot } from "../src/core";
import { collect } from "../src/egress";
import { get, parseCommand, parseU64 } from "../src/flat";
import { Args, die, fail, partitionMap, print, readText } from "../src/harness";
import { Pipeline, Snapshot, Status } from "../src/pipeline";
import { readSnapshot, recover, restore } from "../src/recover";
import { listCheckpoints, repairDir } from "../src/journal";
import { PartitionMap } from "../src/routing";

function tailForm(snapPath: string, tailPath: string, map: PartitionMap): void {
  let snap: Snapshot;
  let book;
  try {
    snap = readSnapshot(snapPath);
    book = restore(CORES.fifo, defaultConfig(), map, snap).book; // validates every book
    parseSnapshot(snap.body);
  } catch (e) {
    return die((e as Error).message);
  }
  const cmds: Array<[number, Command]> = [];
  readText(tailPath).split("\n").forEach((raw, i) => {
    const line = raw.replace(/\r$/, "");
    if (line === "" || line.includes('"format"')) return;
    const cmd = parseCommand(line);
    const sf = get(line, "symbol");
    const sym = sf === undefined ? 0 : parseU64(sf);
    if (cmd === undefined || sym === undefined || sym > 0xffffffff) die(`${tailPath}:${i + 1}: malformed journal line: ${line}`);
    cmds.push([sym, cmd]);
  });
  const [f, events] = collect(true);
  try {
    const p = Pipeline.builder().bookConfig(book).partitionMap(map).egress(f)
      .initial({ snapshot: { body: snap.body, iseq: snap.iseq }, nextIseq: snap.iseq + 1 }).build();
    if (p.publishBatch(cmds) !== Status.Ok) fail("pipeline closed");
    p.drain();
    p.shutdown();
  } catch (e) {
    return fail((e as Error).message);
  }
  print(events.listing());
}

function journalForm(dir: string, snapPath: string | undefined, binary: boolean, repair: boolean, map: PartitionMap): void {
  const parts: string[][] = Array.from({ length: map.partitions }, () => []);
  try {
    if (repair) for (const r of repairDir(dir, binary ? "binary" : "jsonl")) process.stderr.write(`repaired ${r.path} ${r.bytes}\n`);
    const cps = listCheckpoints(dir);
    const snapFile = snapPath ?? (cps.length > 0 ? cps[cps.length - 1].path : undefined);
    const snap = snapFile === undefined ? undefined : readSnapshot(snapFile);
    recover(CORES.fifo, defaultConfig(), map, snap, { dir, format: binary ? "binary" : "jsonl" },
      (p, s, seq, ev) => parts[p].push(eventCanonical(seq, ev, s) + "\n"));
  } catch (e) {
    return die((e as Error).message);
  }
  print(parts.map((p) => p.join("")).join(""));
}

const usage = "orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]\n" +
  "       orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]";
const a = new Args(process.argv.slice(2), usage, ["--partitions", "--partition-map", "--journal-dir", "--snap"], ["--binary", "--repair"]);
const map = partitionMap(a);
const dir = a.get("--journal-dir");
if (dir !== undefined && a.positional.length === 0) journalForm(dir, a.get("--snap"), a.flag("--binary"), a.flag("--repair"), map);
else if (dir === undefined && a.positional.length === 2 && a.get("--snap") === undefined && !a.flag("--binary") && !a.flag("--repair"))
  tailForm(a.positional[0], a.positional[1], map);
else die(usage);
