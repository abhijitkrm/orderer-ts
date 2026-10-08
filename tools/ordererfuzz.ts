//! ordererfuzz — spec/HARNESS.md §4.2 (mirrors matcherfuzz).
//!   ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//! Like orderrun, but symbol-tagged only for engine files.
import { Args, COMMON_FLAGS, COMMON_VALUED, common, die, loadCorpus, print, runCorpus } from "../src/harness";

const usage = "ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";
const a = new Args(process.argv.slice(2), usage, COMMON_VALUED, COMMON_FLAGS);
if (a.positional.length !== 1) die(usage);
const c = loadCorpus(a.positional[0]);
print(runCorpus(c, common(a), c.engine, false).listing);
