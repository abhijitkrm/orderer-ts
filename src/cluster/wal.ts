// The replicated log's write-ahead file, byte-compatible with orderer-rust
// (wal.rs), orderer-cpp and orderer-go; spec/REPLICATION.md 13.3.
//
// One consensus write (Persist) is one frame: len u32 | crc32c u32 | body.
// Recovery cuts a torn tail (including a zero-filled one that a SIGKILL during
// an append can leave) and refuses to start when valid frames follow a bad one.

import * as fs from "fs";
import * as path from "path";
import { Disk, NONE, Persist } from "../consensus";
import { applyPersist, CodecError, decodePersist, encodePersist, Out } from "./codec";
import { crc32c } from "./crc";

const MAGIC = Buffer.from([0x4f, 0x52, 0x44, 0x57, 0x41, 0x4c, 0, 1]); // "ORDWAL\0\1"
const FRAME_HEAD = 8;
const MAX_FRAME = 256 << 20;
// term u32, vote u8, compact flag u8, from u64, entry count u32.
const MIN_BODY = 4 + 1 + 1 + 8 + 4;

export type WalErrKind = "io" | "corrupt" | "badheader";

export class WalError extends Error {
  constructor(readonly kind: WalErrKind, msg: string, readonly offset = 0) {
    super(msg);
  }
}

export interface Repair { frames: number; tornBytes: number }

const io = (m: string) => new WalError("io", m);

/// fsync a descriptor (libuv uses F_FULLFSYNC on macOS).
export function syncFd(fd: number): void {
  try {
    fs.fsyncSync(fd);
  } catch (e) {
    throw io(`fsync: ${(e as Error).message}`);
  }
}

export function syncDir(dir: string): void {
  let fd: number;
  try {
    fd = fs.openSync(dir, "r");
  } catch (e) {
    throw io(`open dir ${dir}: ${(e as Error).message}`);
  }
  try {
    fs.fsyncSync(fd);
  } catch (e) {
    // Some platforms refuse fsync on a directory handle; the rename is as
    // durable as that platform can make it then.
    const code = (e as NodeJS.ErrnoException).code;
    if (code !== "EINVAL" && code !== "ENOTSUP") throw io(`fsync dir ${dir}: ${(e as Error).message}`);
  } finally {
    fs.closeSync(fd);
  }
}

export function writeAll(fd: number, b: Uint8Array): void {
  let off = 0;
  while (off < b.length) {
    try {
      off += fs.writeSync(fd, b, off, b.length - off);
    } catch (e) {
      throw io(`write: ${(e as Error).message}`);
    }
  }
}

/// Create `p` holding `parts`, fsynced.
export function writeFileDurably(p: string, ...parts: Uint8Array[]): void {
  let fd: number;
  try {
    fd = fs.openSync(p, "w");
  } catch (e) {
    throw io(`create ${p}: ${(e as Error).message}`);
  }
  try {
    for (const b of parts) writeAll(fd, b);
    syncFd(fd);
  } finally {
    fs.closeSync(fd);
  }
}

function frame(p: Persist, out: Out): void {
  const start = out.len;
  out.u32(0).u32(0);
  encodePersist(p, out);
  const body = out.buf.subarray(start + FRAME_HEAD, out.len);
  out.buf.writeUInt32LE(body.length, start);
  out.buf.writeUInt32LE(crc32c(body), start + 4);
}

function frameAt(d: Buffer, off: number): Buffer | undefined {
  if (off + FRAME_HEAD > d.length) return undefined;
  const l = d.readUInt32LE(off), c = d.readUInt32LE(off + 4);
  // A real body is never shorter than the fixed fields of a write. That also
  // rejects zeros: an all-zero header (length 0, checksum 0) would otherwise
  // pass as a valid empty frame.
  if (l < MIN_BODY || l > MAX_FRAME || off + FRAME_HEAD + l > d.length) return undefined;
  const body = d.subarray(off + FRAME_HEAD, off + FRAME_HEAD + l);
  return crc32c(body) === c ? body : undefined;
}

function fits(disk: Disk, p: Persist): boolean {
  if (p.from === 0) return false;
  let base = disk.base, n = disk.log.length;
  if (p.compact && p.compact.index > disk.base) {
    base = p.compact.index;
    const drop = p.compact.index - disk.base;
    n = n > drop ? n - drop : 0;
  }
  return p.from > base && p.from - 1 - base <= n;
}

function validFrameFollows(d: Buffer, bad: number): boolean {
  if (bad + FRAME_HEAD <= d.length) {
    const l = d.readUInt32LE(bad);
    if (l >= MIN_BODY && l <= MAX_FRAME) {
      const next = bad + FRAME_HEAD + l;
      if (next < d.length && frameAt(d, next)) return true;
    }
  }
  // Bounded scan for a frame starting anywhere in the next 1 MiB.
  const end = Math.min(d.length, bad + 1 + (1 << 20));
  for (let o = bad + 1; o < end; o++) {
    if (o + 4 > d.length) break;
    if (d.readUInt32LE(o) > d.length - o) continue; // cheap reject before checksumming
    const body = frameAt(d, o);
    if (body) {
      try {
        decodePersist(body);
        return true;
      } catch (e) {
        if (!(e instanceof CodecError)) throw e;
      }
    }
  }
  return false;
}

/// Replay a whole WAL image. `valid` is the length of the intact prefix.
export function replayWal(data: Buffer): { disk: Disk; valid: number; frames: number } {
  if (data.length < MAGIC.length || !data.subarray(0, MAGIC.length).equals(MAGIC))
    throw new WalError("badheader", "wal header missing or unrecognised");
  const disk: Disk = { term: 0, votedFor: NONE, log: [], base: 0, baseTerm: 0 };
  let off = MAGIC.length, frames = 0;
  while (off < data.length) {
    const body = frameAt(data, off);
    if (!body) {
      // Torn only if nothing valid follows.
      if (validFrameFollows(data, off)) throw new WalError("corrupt", "wal corrupt", off);
      break;
    }
    let p: Persist;
    try {
      p = decodePersist(body);
    } catch (e) {
      if (e instanceof CodecError) throw new WalError("corrupt", "wal corrupt", off);
      throw e;
    }
    if (!fits(disk, p)) throw new WalError("corrupt", "wal corrupt", off);
    applyPersist(disk, p);
    off += FRAME_HEAD + body.length;
    frames++;
  }
  return { disk, valid: off, frames };
}

export class Wal {
  private fd: number;
  private pending: Buffer[] = [];
  private constructor(fd: number, private readonly file: string, public len: number) {
    this.fd = fd;
  }

  /// Open (creating if absent) the WAL in `dir`, repairing a torn tail.
  static open(dir: string): { wal: Wal; disk: Disk; repair: Repair } {
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch (e) {
      throw io(`create ${dir}: ${(e as Error).message}`);
    }
    const file = path.join(dir, "raft.wal"), tmp = path.join(dir, "raft.wal.tmp");
    fs.rmSync(tmp, { force: true });
    if (!fs.existsSync(file)) {
      writeFileDurably(tmp, MAGIC);
      fs.renameSync(tmp, file);
      syncDir(dir);
    }
    let data: Buffer;
    try {
      data = fs.readFileSync(file);
    } catch (e) {
      throw io(`cannot read ${file}: ${(e as Error).message}`);
    }
    const { disk, valid, frames } = replayWal(data);
    const torn = data.length - valid;
    const fd = fs.openSync(file, "r+");
    if (torn > 0) {
      fs.ftruncateSync(fd, valid);
      syncFd(fd);
    }
    return { wal: new Wal(fd, file, valid), disk, repair: { frames, tornBytes: torn } };
  }

  /// Buffer one write. Nothing is durable until it is synced.
  append(p: Persist): void {
    const o = new Out(256);
    frame(p, o);
    this.pending.push(o.done());
  }

  /// Write buffered frames to the file (not yet durable).
  flush(): void {
    if (this.pending.length === 0) return;
    const b = this.pending.length === 1 ? this.pending[0] : Buffer.concat(this.pending);
    this.pending = [];
    // Positional write: the file offset is ours to track.
    let off = 0;
    while (off < b.length) {
      try {
        off += fs.writeSync(this.fd, b, off, b.length - off, this.len + off);
      } catch (e) {
        throw io(`write: ${(e as Error).message}`);
      }
    }
    this.len += b.length;
  }

  /// Start an fsync that finishes off the event loop.
  syncAsync(done: (err?: Error) => void): void {
    fs.fsync(this.fd, (e) => done(e ?? undefined));
  }

  sync(durable = true): void {
    this.flush();
    if (durable) syncFd(this.fd);
  }

  /// Replace the file with one frame holding the current state, so a
  /// compacted log's prefix no longer occupies disk. Crash-safe.
  rewrite(): void {
    this.sync(false);
    const { disk } = replayWal(fs.readFileSync(this.file));
    const p: Persist = {
      token: 0, term: disk.term, votedFor: disk.votedFor, from: disk.base + 1, entries: disk.log,
      compact: disk.base > 0 ? { index: disk.base, term: disk.baseTerm } : null,
    };
    const o = new Out(256);
    o.bytes(MAGIC);
    frame(p, o);
    const out = o.done();
    const dir = path.dirname(this.file), tmp = path.join(dir, "raft.wal.tmp");
    writeFileDurably(tmp, out);
    fs.renameSync(tmp, this.file);
    syncDir(dir);
    fs.closeSync(this.fd);
    this.fd = fs.openSync(this.file, "r+");
    this.len = out.length;
  }

  close(): void {
    if (this.fd >= 0) {
      fs.closeSync(this.fd);
      this.fd = -1;
    }
  }
}
