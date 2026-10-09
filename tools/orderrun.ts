//! orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//!   orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
//!            [--checkpoint-every K] [--durable]
//! Runs the file through a pipeline (one producer, file order), drains, prints
//! every event symbol-tagged, grouped by partition. --snap then writes the
//! merged snapshot to PATH and its cut to PATH.meta.
import { Args, COMMON_FLAGS, COMMON_VALUED, common, die, loadCorpus, print, runCorpus } from "../src/harness";

const usage = "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH] [--checkpoint-every K] [--durable]";
const a = new Args(process.argv.slice(2), usage, [...COMMON_VALUED, "--snap", "--checkpoint-every"], [...COMMON_FLAGS, "--durable"]);
if (a.positional.length !== 1) die(usage);
const c = loadCorpus(a.positional[0]);
const com = common(a);
const snapPath = a.get("--snap");
const r = runCorpus(c, com, true, {
  snapshot: snapPath !== undefined,
  checkpointEvery: a.get("--checkpoint-every") === undefined ? undefined : a.num("--checkpoint-every", 0, Number.MAX_SAFE_INTEGER),
  durable: a.flag("--durable"),
});
print(r.listing);
if (snapPath !== undefined) r.snap!.write(snapPath);
