// The runtime's storage and wire layer against fixtures written by
// orderer-rust (vectors/replication-wire, from `wirefix`): messages, WAL frame
// bodies, command batches, WAL files in every damaged state, and snapshots.
// Decoding must give the same values, re-encoding the same bytes, recovery the
// same disk image, and files written here must equal the Rust files byte for
// byte.
//
//   node dist/tests/cluster_wire.js [vectors-dir]
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Disk, Entry, Msg, NONE, Persist } from "../src/consensus";
import { Session } from "../src/cluster/auth";
import { CodecError, decodeBatch, decodeMsg, decodePersist, encodeBatch, encodeMsg, encodePersist, sessionOf } from "../src/cluster/codec";
import { crc32c, crc32cUpdate } from "../src/cluster/crc";
import { FrameReader, NetError, putFrame } from "../src/cluster/net";
import { SnapStore } from "../src/cluster/snapstore";
import { Wal, WalError } from "../src/cluster/wal";
import { check, runAll } from "./t";

const WIRE = path.join(process.argv[2] ?? "vectors", "replication-wire");
const hexs = (d: Uint8Array) => (d.length === 0 ? "-" : Buffer.from(d).toString("hex"));
const unhex = (s: string) => (s === "-" || s === "" ? Buffer.alloc(0) : Buffer.from(s, "hex"));
const fmtEntries = (es: Entry[]) => (es.length === 0 ? "-" : es.map((e) => `${e.term}:${hexs(e.data)}`).join(","));
const b = (x: boolean) => (x ? 1 : 0);

function fmtMsg(m: Msg): string {
  switch (m.kind) {
    case "vote": return `vote ${m.term} ${m.lastIndex} ${m.lastTerm} ${b(m.pre)}`;
    case "votereply": return `votereply ${m.term} ${b(m.granted)} ${b(m.pre)}`;
    case "append": return `append ${m.term} ${m.prevIndex} ${m.prevTerm} ${m.commit} ${fmtEntries(m.entries)}`;
    case "appendreply": return `appendreply ${m.term} ${b(m.success)} ${m.index}`;
  }
}

const fmtDisk = (d: Disk) => `${d.term}:${d.votedFor === NONE ? "-" : d.votedFor}:${d.base}:${d.baseTerm}:${fmtEntries(d.log)}`;

function fixtureLines(name: string): Array<[string, string]> {
  const out: Array<[string, string]> = [];
  for (const line of fs.readFileSync(path.join(WIRE, name), "utf8").split("\n")) {
    const i = line.indexOf(" | ");
    if (i >= 0) out.push([line.slice(0, i), line.slice(i + 3)]);
  }
  return out;
}

const scratch = (n: string) => fs.mkdtempSync(path.join(os.tmpdir(), `orderer-ts-wire-${n}-`));
const rm = (d: string) => fs.rmSync(d, { recursive: true, force: true });
const slurpB = (p: string) => fs.readFileSync(p);

function throws(f: () => void, cls: new (...a: never[]) => Error = CodecError as never): boolean {
  try {
    f();
  } catch (e) {
    return e instanceof cls;
  }
  return false;
}

const tests: Array<[string, () => void]> = [
  ["crc32c matches the check value and is incremental", () => {
    check(crc32c(Buffer.from("123456789")) === 0xe3069283, "check value");
    const data = Buffer.alloc(5000);
    let x = 7;
    for (let i = 0; i < data.length; i++) { x = (x * 1103515245 + 12345) >>> 0; data[i] = x >>> 24; }
    for (const step of [1, 3, 8, 13, 999]) {
      let c = 0;
      for (let i = 0; i < data.length; i += step) c = crc32cUpdate(c, data, i, Math.min(i + step, data.length));
      check(c === crc32c(data), "step ", step);
    }
  }],

  ["sessions detect tampering, replay, reordering and reflection", () => {
    const k = Buffer.alloc(32, 7);
    const a = new Session(k, 0), bb = new Session(k, 1);
    const one = Buffer.from("one"), two = Buffer.from("two"), twp = Buffer.from("twp");
    const m1 = a.seal(1, one), m2 = a.seal(1, two);
    check(bb.open(1, one, m1), "first frame");
    check(!bb.open(1, one, m1), "replay");
    check(!bb.open(1, twp, m2), "altered body");
    check(!bb.open(2, two, m2), "altered kind");
    check(bb.open(1, two, m2), "rejections must not advance state");
    a.seal(1, one);
    const m4 = a.seal(1, one);
    check(!bb.open(1, one, m4), "a dropped frame");
    const a2 = new Session(k, 0);
    check(!a2.open(1, one, a2.seal(1, one)), "reflection");
    check(!new Session(Buffer.alloc(32, 8), 1).open(1, one, new Session(k, 0).seal(1, one)), "wrong key");
  }],

  ["frames round-trip across arbitrary chunking, and damage is refused", () => {
    const k = Buffer.alloc(32, 3);
    const seal = () => {
      const tx = new Session(k, 0);
      return Buffer.concat([putFrame(1, Buffer.from("hello"), tx), putFrame(5, Buffer.alloc(0), tx), putFrame(3, Buffer.alloc(100000, 9), tx)]);
    };
    const all = seal();
    for (const step of [1, 7, 4096, all.length]) {
      const r = new FrameReader();
      r.sess = new Session(k, 1); // sessions are stateful: fresh for every pass
      const got: Array<[number, number]> = [];
      for (let i = 0; i < all.length; i += step) r.push(all.subarray(i, Math.min(i + step, all.length)), (f) => got.push([f.kind, f.body.length]));
      check(JSON.stringify(got) === "[[1,5],[5,0],[3,100000]]", "chunking ", step, " ", JSON.stringify(got));
    }
    const bad = Buffer.from(putFrame(1, Buffer.from("x")));
    bad[bad.length - 1] ^= 1;
    check(throws(() => new FrameReader().push(bad, () => {}), NetError as never), "checksum");
    check(throws(() => new FrameReader().push(Buffer.alloc(8), () => {}), NetError as never), "zero length");
  }],

  ["messages decode and re-encode exactly as Rust wrote them", () => {
    const lines = fixtureLines("msgs.txt");
    check(lines.length >= 8, "only ", lines.length, " message fixtures");
    for (const [hx, text] of lines) {
      const bytes = unhex(hx);
      const m = decodeMsg(bytes);
      check(fmtMsg(m) === text, "decoded ", fmtMsg(m), " want ", text);
      check(encodeMsg(m).done().equals(bytes), "re-encode ", text);
      for (let n = 0; n < bytes.length; n++) check(throws(() => decodeMsg(bytes.subarray(0, n))), text, " prefix ", n, " decoded");
    }
  }],

  ["hostile input never crashes the decoders", () => {
    let x = 99;
    const rnd = () => { x = (x * 1664525 + 1013904223) >>> 0; return x; };
    for (let i = 0; i < 20000; i++) {
      const junk = Buffer.alloc(rnd() % 64);
      for (let j = 0; j < junk.length; j++) junk[j] = rnd() >>> 24;
      for (const f of [() => decodeMsg(junk), () => decodePersist(junk), () => decodeBatch(junk)]) {
        try { f(); } catch (e) { if (!(e instanceof CodecError)) throw e; }
      }
    }
    const evil = Buffer.concat([Buffer.from([3]), Buffer.alloc(24), Buffer.from([0xff, 0xff, 0xff, 0xff])]);
    check(throws(() => decodeMsg(evil)), "hostile entry count accepted");
  }],

  ["WAL frame bodies round-trip", () => {
    for (const [hx, text] of fixtureLines("persists.txt")) {
      const body = unhex(hx);
      const p = decodePersist(body);
      const got = `${p.term} ${p.votedFor === NONE ? "-" : p.votedFor} ${p.from} ${p.compact ? `${p.compact.index}:${p.compact.term}` : "-"} ${fmtEntries(p.entries)}`;
      check(got === text, "decoded ", got, " want ", text);
      check(encodePersist(p).done().equals(body), "re-encode ", text);
    }
  }],

  ["command batches round-trip, including sessions", () => {
    const tifs = ["gtc", "ioc", "fok", "post_only"];
    for (const [hx, rest] of fixtureLines("batches.txt")) {
      const body = unhex(hx);
      const words = rest.split(/\s+/).filter((w) => w !== "");
      const cmds = decodeBatch(body);
      const s = sessionOf(body);
      if (words[0] === "-") check(s === undefined, "plain batch decoded as a session");
      else check(s !== undefined && s.client === BigInt(words[0]) && s.seq === BigInt(words[1]), "session ", words[0], " ", words[1]);
      const want = words.slice(3); // "client seq |" then commands
      check(cmds.length === want.length, cmds.length, " commands, want ", want.length);
      cmds.forEach(([sym, c], i) => {
        let got = `${sym}:${c.kind}:`;
        got += c.kind === "new" ? `${c.side === 0 ? "bid" : "ask"}:${c.otype === 0 ? "limit" : "market"}:${tifs[c.tif]}` : "-:-:-";
        got += `:${c.orderId}:${c.kind === "cancel" ? 0 : c.price}:${c.kind === "cancel" ? 0 : c.qty}`;
        check(got === want[i], "command ", i, ": ", got, " want ", want[i]);
      });
      if (body.length > 0) check(encodeBatch(cmds, s).equals(body), "re-encode ", hx.slice(0, 40));
    }
  }],

  ["WAL recovery matches Rust for every damage pattern", () => {
    let cases = 0;
    for (const f of fs.readdirSync(WIRE).sort()) {
      const m = /^wal-(.*)\.bin$/.exec(f);
      if (!m) continue;
      const tag = m[1];
      const want = fs.readFileSync(path.join(WIRE, `wal-${tag}.expect`), "utf8").replace(/\n+$/, "");
      const dir = scratch(tag);
      fs.copyFileSync(path.join(WIRE, f), path.join(dir, "raft.wal"));
      let got: string;
      try {
        const { wal, disk, repair } = Wal.open(dir);
        got = `ok ${fmtDisk(disk)} frames=${repair.frames} torn=${repair.tornBytes}`;
        wal.close();
      } catch (e) {
        if (!(e instanceof WalError)) throw e;
        got = e.kind === "corrupt" ? `corrupt ${e.offset}` : e.kind === "badheader" ? "badheader" : `io ${e.message}`;
      }
      check(got === want, tag, "\n  want ", want, "\n  got  ", got);
      rm(dir);
      cases++;
    }
    check(cases >= 15, "only ", cases, " WAL fixtures");
  }],

  ["a WAL written here is byte-identical to the one Rust wrote", () => {
    const e = (term: number, ...d: number[]): Entry => ({ term, data: Buffer.from(d) });
    const mk = (term: number, votedFor: number, from: number, entries: Entry[], compact: Persist["compact"] = null): Persist =>
      ({ token: 0, term, votedFor, from, entries, compact });
    const dir = scratch("identical");
    let { wal } = Wal.open(dir);
    wal.append(mk(1, 1, 1, [e(1, 1), e(1, 2), e(1, 3)]));
    wal.append(mk(2, 2, 3, [e(2, 9, 9)]));
    wal.append(mk(2, 2, 4, [e(2), e(2, 4)]));
    wal.append(mk(3, NONE, 6, [], { index: 3, term: 2 }));
    wal.sync();
    wal.close();
    check(slurpB(path.join(dir, "raft.wal")).equals(slurpB(path.join(WIRE, "wal-good.bin"))), "WAL bytes differ from Rust's");
    // Appending after recovery lands cleanly; a rewrite keeps the state.
    wal = Wal.open(dir).wal;
    wal.append(mk(3, NONE, 6, [e(3, 7)]));
    wal.sync();
    wal.rewrite();
    wal.append(mk(3, NONE, 7, [e(3, 8)]));
    wal.sync();
    wal.close();
    const r = Wal.open(dir);
    check(r.disk.base === 3 && r.disk.baseTerm === 2 && r.disk.log.length === 4 && r.repair.tornBytes === 0, fmtDisk(r.disk));
    r.wal.close();
    rm(dir);
  }],

  ["snapshots read and write identically to Rust", () => {
    const dir = scratch("snap");
    const store = new SnapStore(dir);
    for (const idx of ["42", "7"]) {
      const f = fs.readFileSync(path.join(WIRE, `snap-${idx}.expect`), "utf8").trim().split(/\s+/);
      const index = Number(f[0]), term = Number(f[1]), len = Number(f[2]);
      const data = unhex(f[4] ?? "");
      const name = `snap-${String(index).padStart(20, "0")}.snap`;
      fs.copyFileSync(path.join(WIRE, name), path.join(dir, name));
      const got = store.get(index);
      check(got !== undefined && got.meta.term === term && got.meta.len === len && got.data.equals(data), "snapshot ", idx);
      const dir2 = scratch(`snap2-${idx}`);
      new SnapStore(dir2).put(index, term, data);
      check(slurpB(path.join(dir2, name)).equals(slurpB(path.join(WIRE, name))), "snapshot ", idx, " differs from Rust's");
      rm(dir2);
    }
    check(JSON.stringify(store.list()) === "[7,42]", "list ", JSON.stringify(store.list()));
    // Corruption and truncation are detected.
    const p = store.path(42);
    const bytes = slurpB(p);
    const flipped = Buffer.from(bytes);
    flipped[flipped.length - 1] ^= 1;
    fs.writeFileSync(p, flipped);
    check(store.get(42) === undefined, "bit flip");
    fs.writeFileSync(p, flipped.subarray(0, flipped.length - 3));
    check(store.get(42) === undefined, "truncation");
    // Streaming receive verifies length and checksum.
    const blob = Buffer.alloc(100000);
    for (let i = 0; i < blob.length; i++) blob[i] = i & 255;
    const crc = crc32c(blob);
    const inc = store.receive(9, 1, blob.length, crc);
    for (let i = 0; i < blob.length; i += 4096) inc.write(blob.subarray(i, Math.min(i + 4096, blob.length)));
    inc.finish();
    check(store.get(9)?.data.equals(blob) === true, "received snapshot unreadable");
    const bad = store.receive(10, 1, 4, crc);
    bad.write(Buffer.from("abcd"));
    check(throws(() => bad.finish(), WalError as never) && store.get(10) === undefined, "checksum mismatch accepted");
    rm(dir);
  }],
];

runAll(tests);
