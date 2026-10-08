//! ordersnap — spec/HARNESS.md §4.4 (mirrors matchersnap).
//!   ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//! Runs the file, drains, prints the merged matcher-snap/1 snapshot.
import { Args, COMMON_FLAGS, COMMON_VALUED, common, die, loadCorpus, print, runCorpus } from "../src/harness";

const usage = "ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";
const a = new Args(process.argv.slice(2), usage, COMMON_VALUED, COMMON_FLAGS);
if (a.positional.length !== 1) die(usage);
const c = loadCorpus(a.positional[0]);
print(runCorpus(c, common(a), true, true).snap!.body);
