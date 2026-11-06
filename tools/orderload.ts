//! orderload — load generator, cluster inspector and failover verifier (the
//! TypeScript port of orderer-rust's orderer-load; same modes, flags and output).
//!
//!   orderload run    --clients A,B,C --secs S --acked FILE [--maybe FILE] [--batch N] [--threads T]
//!   orderload status --clients A,B,C
//!   orderload settle --clients A,B,C [--timeout S]
//!   orderload verify --acked FILE [--maybe FILE] --applied F1,F2,...
//!
//! `run` writes the id of every acknowledged batch to the acked file as soon as
//! it is acknowledged, and ids with an unknown outcome to the maybe file.
//! `verify` then proves that nothing acknowledged was lost, nothing was applied
//! twice or invented, and every replica applied the same sequence. ORDERER_KEY
//! in the environment sets the pre-shared cluster key.
//!
//! Batch ids are ((thread << 36 | seq) << 12), so every order id stays below
//! 2^53, the range matcher-ts represents exactly; the Rust and Go tools use a
//! wider layout that a TypeScript replica cannot hold.
import * as fs from "fs";
import { Client, CmdSym, Conn } from "../src/cluster";
import { newLimit, Side, Tif } from "../src/matcher/types";

function die(msg: string): never {
  process.stderr.write(`orderload: ${msg}\n`);
  process.exit(2);
}

const key = (): Buffer | undefined => (process.env.ORDERER_KEY ? Buffer.from(process.env.ORDERER_KEY) : undefined);
const members = (s: string) => s.split(",").map((addr, id) => (addr.includes(":") ? { id, addr } : die(`bad address ${addr}`)));
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function readIds(p: string): number[] {
  let t = "";
  try { t = fs.readFileSync(p, "utf8"); } catch { /* missing file = no ids */ }
  return t.split("\n").map((l) => l.trim()).filter((l) => /^\d+$/.test(l)).map(Number);
}

function readApplied(p: string): Array<[number, number]> {
  let b: Buffer;
  try { b = fs.readFileSync(p); } catch (e) { return die(`${p}: ${(e as Error).message}`); }
  const out: Array<[number, number]> = [];
  for (let o = 0; o + 16 <= b.length; o += 16) out.push([Number(b.readBigUInt64LE(o)), Number(b.readBigUInt64LE(o + 8))]);
  return out;
}

async function run(clients: string, secs: number, acked: string, maybe: string, batch: number, threads: number): Promise<void> {
  if (!acked) die("--acked is required");
  const ackedFd = fs.openSync(acked, "a");
  const maybeFd = maybe ? fs.openSync(maybe, "a") : undefined;
  const end = Date.now() + secs * 1000;
  let ok = 0, unknown = 0, failed = 0;
  await Promise.all(Array.from({ length: threads }, async (_, t) => {
    // One session per thread: retries are exactly-once.
    const c = new Client(members(clients)).withSession(t + 1);
    const k = key();
    if (k) c.withKey(k);
    let x = (0x9e3779b9 ^ ((t + 1) * 2654435761)) >>> 0 || 1;
    for (let seq = 1; Date.now() < end; seq++) {
      // The batch id doubles as its first order's id; the rest are derived from it.
      const id = (((t + 1) * 2 ** 36) + seq) * 4096;
      const cmds: CmdSym[] = [];
      for (let j = 0; j < batch; j++) {
        x ^= x << 13; x >>>= 0; x ^= x >>> 17; x ^= x << 5; x >>>= 0;
        cmds.push([x % 16, newLimit(id + j, x & 1 ? Side.Ask : Side.Bid, 10_000 + ((x >>> 8) % 8) - 4, 1 + ((x >>> 20) % 10), Tif.Gtc)]);
      }
      const r = await c.proposeSession(cmds);
      if (r.kind === "ok") { fs.writeSync(ackedFd, `${id}\n`); ok++; }
      else if (r.kind === "unknown") { if (maybeFd !== undefined) fs.writeSync(maybeFd, `${id}\n`); unknown++; }
      else failed++;
    }
    c.close();
  }));
  console.log(`load finished: ${ok} acknowledged, ${unknown} unknown, ${failed} not submitted (${batch} commands each)`);
}

async function status(clients: string): Promise<void> {
  for (const m of members(clients)) {
    const c = await Conn.connect(m.addr, key());
    if (!c) { console.log(`${m.addr}: unreachable (connect failed)`); continue; }
    const s = await c.status();
    process.stdout.write(s !== undefined ? `${m.addr}: ${s}` : `${m.addr}: unreachable (no status)\n`);
    c.close();
  }
}

const field = (s: string, k: string) => s.split(/\s+/).find((kv) => kv.startsWith(`${k}=`))?.slice(k.length + 1);

/// Wait until every node has applied everything committed and all agree.
async function settle(clients: string, timeout: number): Promise<void> {
  const ms = members(clients);
  const end = Date.now() + timeout * 1000;
  for (;;) {
    const stats: string[] = [], hashes: string[] = [];
    let ok = true;
    for (const m of ms) {
      const c = await Conn.connect(m.addr, key());
      if (!c) { ok = false; continue; }
      const s = await c.status();
      const h = await c.hash();
      c.close();
      if (s === undefined || !h) { ok = false; continue; }
      stats.push(s);
      hashes.push(`${h.index} ${h.digest.toString("hex")}`);
    }
    if (ok && stats.length === ms.length) {
      if (stats.some((s) => { const h = field(s, "halted"); return h !== undefined && h !== "-"; })) { console.log("FAIL: a node halted"); process.exit(1); }
      const agree = stats.every((s, i) => field(s, "applied") === field(stats[0], "applied") && field(s, "commit") === field(stats[0], "commit") &&
        field(s, "applied") === field(s, "commit") && hashes[i] === hashes[0]);
      if (agree) {
        const [idx, d] = hashes[0].split(" ");
        console.log(`settled: all ${ms.length} nodes at index ${idx}, state digest ${d}`);
        return;
      }
    }
    if (Date.now() > end) {
      console.log(`FAIL: nodes did not settle within ${timeout}s`);
      await status(clients);
      process.exit(1);
    }
    await sleep(200);
  }
}

function verify(acked: string, maybe: string, applied: string): void {
  const ackedIds = readIds(acked);
  const maybeSet = new Set(readIds(maybe));
  const ackedSet = new Set(ackedIds);
  if (ackedSet.size !== ackedIds.length) { console.log("FAIL: the load generator acknowledged an id twice"); process.exit(1); }
  let first: Array<[number, number]> | undefined;
  let failures = 0;
  for (const p of applied.split(",")) {
    const log = readApplied(p);
    const set = new Set(log.map((l) => l[1]));
    const bad: string[] = [];
    if (set.size !== log.length) bad.push("a batch was applied twice");
    if (!log.every((l, i) => i === 0 || log[i - 1][0] < l[0])) bad.push("log indexes are not increasing");
    let missing = 0;
    for (const a of ackedSet) if (!set.has(a)) missing++;
    if (missing > 0) bad.push(`${missing} acknowledged batches are missing`);
    let invented = 0;
    for (const i of set) if (!ackedSet.has(i) && !maybeSet.has(i)) invented++;
    if (invented > 0) bad.push(`${invented} applied batches were never submitted`);
    if (first === undefined) first = log;
    else if (first.length !== log.length || first.some((f, i) => f[0] !== log[i][0] || f[1] !== log[i][1])) bad.push("applied sequence differs from the first node's");
    if (bad.length === 0) console.log(`${p}: ok (${log.length} batches applied, ${ackedSet.size} acknowledged all present)`);
    else { failures++; console.log(`${p}: FAIL: ${bad.join("; ")}`); }
  }
  if (failures > 0) process.exit(1);
  console.log("PASS: every acknowledged batch survived on every replica, in the same order, exactly once");
}

async function main(): Promise<void> {
  const mode = process.argv[2] ?? die("mode: run|status|settle|verify");
  const o: Record<string, string> = {};
  for (let i = 3; i < process.argv.length; i += 2) {
    if (!process.argv[i].startsWith("--") || process.argv[i + 1] === undefined) die(`bad argument ${process.argv[i]}`);
    o[process.argv[i].slice(2)] = process.argv[i + 1];
  }
  const num = (k: string, d: number) => (o[k] === undefined ? d : Number.isFinite(Number(o[k])) ? Number(o[k]) : die(`--${k} wants a number`));
  switch (mode) {
    case "run": await run(o.clients ?? die("--clients is required"), num("secs", 10), o.acked ?? "", o.maybe ?? "", num("batch", 50), num("threads", 2)); break;
    case "status": await status(o.clients ?? die("--clients is required")); break;
    case "settle": await settle(o.clients ?? die("--clients is required"), num("timeout", 60)); break;
    case "verify": verify(o.acked ?? "", o.maybe ?? "", o.applied ?? ""); break;
    default: die(`unknown mode ${mode}`);
  }
}

void main().then(() => process.exit(process.exitCode ?? 0));
