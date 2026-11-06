// Durable snapshots, byte-compatible with orderer-rust (snapstore.rs),
// orderer-cpp and orderer-go: snap-<index>.snap = "ORDS" | index u64 |
// term u32 | len u64 | crc u32 | data, written to a temp name, fsynced,
// renamed, dir fsynced.

import * as fs from "fs";
import * as path from "path";
import { Out } from "./codec";
import { crc32c, crc32cUpdate } from "./crc";
import { syncDir, syncFd, WalError, writeAll, writeFileDurably } from "./wal";

export const SNAP_HEADER = 4 + 8 + 4 + 8 + 4;

export interface SnapMeta { index: number; term: number; len: number; crc: number }

function head(index: number, term: number, len: number, crc: number): Buffer {
  return new Out(SNAP_HEADER).bytes(Buffer.from("ORDS")).u64(index).u32(term).u64(len).u32(crc).done();
}

function parse(h: Buffer): SnapMeta | undefined {
  if (h.length < SNAP_HEADER || h.toString("latin1", 0, 4) !== "ORDS") return undefined;
  const hi = (o: number) => h.readUInt32LE(o + 4);
  if (hi(4) >= 1 << 21 || hi(16) >= 1 << 21) return undefined;
  return {
    index: hi(4) * 4294967296 + h.readUInt32LE(4), term: h.readUInt32LE(12),
    len: hi(16) * 4294967296 + h.readUInt32LE(16), crc: h.readUInt32LE(24),
  };
}

/// A snapshot being received from a peer, verified before it is published.
export class Incoming {
  private fd: number;
  private got = 0;
  private running = 0;
  constructor(private readonly tmp: string, private readonly dst: string, private readonly dir: string,
    private readonly len: number, private readonly crc: number, header: Buffer) {
    try {
      this.fd = fs.openSync(tmp, "w");
    } catch (e) {
      throw new WalError("io", `create ${tmp}: ${(e as Error).message}`);
    }
    writeAll(this.fd, header);
  }
  write(d: Uint8Array): void {
    if (this.got + d.length > this.len) throw new WalError("io", "snapshot longer than announced");
    this.got += d.length;
    this.running = crc32cUpdate(this.running, d);
    writeAll(this.fd, d);
  }
  /// Verify length and checksum and publish the file.
  finish(): void {
    if (this.got !== this.len || this.running !== this.crc) {
      this.abort();
      throw new WalError("io", this.got !== this.len ? "snapshot shorter than announced" : "snapshot checksum mismatch");
    }
    syncFd(this.fd);
    fs.closeSync(this.fd);
    this.fd = -1;
    fs.renameSync(this.tmp, this.dst);
    syncDir(this.dir);
  }
  abort(): void {
    if (this.fd >= 0) {
      fs.closeSync(this.fd);
      this.fd = -1;
    }
    fs.rmSync(this.tmp, { force: true });
  }
}

export class SnapStore {
  constructor(private readonly dir: string) {
    fs.mkdirSync(dir, { recursive: true });
    // Leftover temp files are incomplete writes.
    for (const f of fs.readdirSync(dir)) if (f.endsWith(".tmp")) fs.rmSync(path.join(dir, f), { force: true });
  }

  private name(i: number): string { return `snap-${String(i).padStart(20, "0")}`; }
  path(i: number): string { return path.join(this.dir, this.name(i) + ".snap"); }

  put(index: number, term: number, data: Uint8Array): void {
    const tmp = path.join(this.dir, this.name(index) + ".tmp");
    writeFileDurably(tmp, head(index, term, data.length, crc32c(data)), data);
    fs.renameSync(tmp, this.path(index));
    syncDir(this.dir);
  }

  /// The header of an intact snapshot file.
  meta(index: number): SnapMeta | undefined {
    try {
      const fd = fs.openSync(this.path(index), "r");
      try {
        const size = fs.fstatSync(fd).size;
        if (size < SNAP_HEADER) return undefined;
        const h = Buffer.alloc(SNAP_HEADER);
        fs.readSync(fd, h, 0, SNAP_HEADER, 0);
        const m = parse(h);
        return m && m.index === index && size === SNAP_HEADER + m.len ? m : undefined;
      } finally {
        fs.closeSync(fd);
      }
    } catch {
      return undefined;
    }
  }

  /// Indexes of intact snapshot files, ascending. A corrupt file is skipped.
  list(): number[] {
    const v: number[] = [];
    for (const f of fs.readdirSync(this.dir)) {
      const m = /^snap-(\d+)\.snap$/.exec(f);
      if (m && this.meta(Number(m[1]))) v.push(Number(m[1]));
    }
    return v.sort((a, b) => a - b);
  }

  /// Load and verify a snapshot; undefined if absent or corrupt.
  get(index: number): { meta: SnapMeta; data: Buffer } | undefined {
    let all: Buffer;
    try {
      all = fs.readFileSync(this.path(index));
    } catch {
      return undefined;
    }
    const m = parse(all);
    if (!m || m.index !== index || all.length - SNAP_HEADER !== m.len) return undefined;
    const data = all.subarray(SNAP_HEADER);
    return crc32c(data) === m.crc ? { meta: m, data } : undefined;
  }

  /// Delete snapshots older than `keepFrom`.
  gc(keepFrom: number): void {
    for (const i of this.list()) if (i < keepFrom) fs.rmSync(this.path(i), { force: true });
  }

  receive(index: number, term: number, len: number, crc: number): Incoming {
    return new Incoming(path.join(this.dir, this.name(index) + ".tmp"), this.path(index), this.dir, len, crc, head(index, term, len, crc));
  }
}
