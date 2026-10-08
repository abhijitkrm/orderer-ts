// A1 — every vendored matcher vector (and orderer's regress vectors), every
// index mode, through the live pipeline: P=1 byte-identical to .evt; P=4
// per-symbol identical (engine) / unchanged (single-book), routing honored.
import * as fs from "fs";
import * as path from "path";
import { IndexKind } from "../src/matcher/priceindex";
import { get, u64 } from "../src/flat";
import { parseCorpus } from "../src/harness";
import { PartitionMap, hashPartition } from "../src/routing";
import { bySymbol, check, concat, eq, lines, runAll, runPipeline, slurp } from "./t";

const VEC = process.argv[2] ?? "vectors";

function runVector(cmd: string, evt: string): number {
  const text = slurp(cmd);
  const c = parseCorpus(text, cmd);
  const expected = lines(slurp(evt)).slice(1);
  const ix = get(text.slice(0, text.indexOf("\n")), "index");
  const modes = ix === "both" ? [IndexKind.Ladder, IndexKind.Tree] : ix === "tree" ? [IndexKind.Tree] : [IndexKind.Ladder];
  for (const index of modes) {
    const cfg = { ...c.book, index };
    check(eq(concat(runPipeline(cfg, c.cmds, 1, c.engine)), expected), `${cmd} P=1`);
    const parts = runPipeline(cfg, c.cmds, 4, c.engine);
    if (c.engine) {
      check(bySymbol(concat(parts)) === bySymbol(expected), `${cmd} P=4`);
      parts.forEach((ls, p) => ls.forEach((l) => check(hashPartition(u64(l, "symbol")!, 4) === p, `routing ${l}`)));
    } else {
      check(eq(concat(parts), expected), `${cmd} P=4 single-book`);
    }
  }
  return modes.length;
}

function everyVectorThroughPipeline(): void {
  let runs = 0;
  const mf = JSON.parse(slurp(path.join(VEC, "matcher/manifest.json")));
  const files: string[] = [];
  JSON.stringify(mf, (k, v) => { if (k === "file") files.push(v); return v; });
  for (const name of files) runs += runVector(path.join(VEC, `matcher/${name}.cmd.jsonl`), path.join(VEC, `matcher/${name}.evt.jsonl`));
  for (const f of fs.readdirSync(path.join(VEC, "regress")))
    if (f.endsWith(".cmd.jsonl")) runs += runVector(path.join(VEC, "regress", f), path.join(VEC, "regress", f.replace(".cmd.jsonl", ".evt.jsonl")));
  check(runs >= 80, `only ${runs} vector runs`);
  console.log(`     ${runs} vector runs`);
}

function routingVectors(): void {
  let n = 0;
  for (const l of lines(slurp(path.join(VEC, "routing/hash.jsonl")))) {
    if (l.includes('"format"')) continue;
    const s = u64(l, "symbol")!, P = u64(l, "partitions")!, want = u64(l, "partition")!;
    check(hashPartition(s, P) === want && PartitionMap.make(P).partition(s) === want, l);
    n++;
  }
  check(n > 500);
  const m = PartitionMap.parseTable(slurp(path.join(VEC, "routing/table.map.jsonl")), 4);
  for (const l of lines(slurp(path.join(VEC, "routing/table.expect.jsonl"))))
    if (!l.includes('"format"')) check(m.partition(u64(l, "symbol")!) === u64(l, "partition"), l);
  let threw = false;
  try {
    PartitionMap.parseTable(slurp(path.join(VEC, "routing/table.map.jsonl")), 3);
  } catch {
    threw = true;
  }
  check(threw, "P mismatch must fail");
}

runAll([
  ["every_vector_through_pipeline", everyVectorThroughPipeline],
  ["routing_vectors", routingVectors],
]);
