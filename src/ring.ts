//! The LMAX Disruptor over SharedArrayBuffer (orderer-rust's
//! orderer-disruptor, ported to worker_threads).
//!
//! A ring is one SharedArrayBuffer: a header of padded 64-bit sequences
//! (cursor, gating cache, one per consumer), per-slot availability laps for
//! multi-producer rings, then fixed-size slots read and written through a
//! DataView. Any worker can attach to a ring from its `RingShared`
//! descriptor (structured-cloned via workerData). Sequences are BigInt64
//! atomics, loaded once per batch; Atomics are sequentially consistent, at
//! least the acquire/release the protocol needs.

export const INITIAL = -1;

/// Wait strategies (not observable).
export type WaitKind = "busySpin" | "yield" | "backoff" | "blocking";
export interface WaitStrategy {
  kind: WaitKind;
  spin: number;
  yields: number;
  parkMinUs: number;
  parkMaxUs: number;
}
export const busySpin = (): WaitStrategy => ({ kind: "busySpin", spin: 0, yields: 0, parkMinUs: 0, parkMaxUs: 0 });
export const yieldWait = (): WaitStrategy => ({ kind: "yield", spin: 100, yields: 0, parkMinUs: 0, parkMaxUs: 0 });
export const backoff = (): WaitStrategy => ({ kind: "backoff", spin: 256, yields: 64, parkMinUs: 20, parkMaxUs: 1000 });
export const blocking = (): WaitStrategy => ({ kind: "blocking", spin: 100, yields: 0, parkMinUs: 0, parkMaxUs: 0 });

export type ProducerKind = "single" | "multi";
export enum Publish {
  Ok,
  Full,
  Alerted,
}

// header layout, in 8-byte words; every sequence sits alone on 128 bytes
const W = 16; // words per padded slot
const CURSOR = 0;
const GATE_CACHE = 1 * W;
const ALERT = 2 * W; // i32 view index ALERT*2: alerted flag
const NOTIFY = 3 * W; // i32 view index NOTIFY*2: waiter count, +1: wake counter
const CONSUMERS = 4 * W;

/// Everything a worker needs to attach to a ring.
export interface RingShared {
  sab: SharedArrayBuffer;
  size: number;
  slotSize: number;
  kind: ProducerKind;
  consumers: number;
  deps: number[][]; // per consumer: upstream consumer indices
  gating: number[]; // consumers the producer waits on
  slotsOffset: number;
  availOffset: number;
  /// Optional shared alert word (a whole pipeline alerted at once).
  globalAlert?: SharedArrayBuffer;
}

/// Parks the calling thread (worker or Node main thread).
const parkCell = new Int32Array(new SharedArrayBuffer(4));
export function parkUs(us: number): void {
  Atomics.wait(parkCell, 0, 0, us / 1000);
}

class Header {
  readonly seq: BigInt64Array;
  readonly i32: Int32Array;
  readonly global?: Int32Array;
  constructor(sh: RingShared) {
    this.seq = new BigInt64Array(sh.sab, 0, CONSUMERS + sh.consumers * W);
    this.i32 = new Int32Array(sh.sab, 0, (CONSUMERS + sh.consumers * W) * 2);
    if (sh.globalAlert) this.global = new Int32Array(sh.globalAlert);
  }
  load(word: number): number {
    return Number(Atomics.load(this.seq, word));
  }
  store(word: number, v: number): void {
    Atomics.store(this.seq, word, BigInt(v));
  }
  alerted(): boolean {
    return Atomics.load(this.i32, ALERT * 2) !== 0 || (this.global !== undefined && Atomics.load(this.global, 0) !== 0);
  }
  alert(): void {
    Atomics.store(this.i32, ALERT * 2, 1);
    this.wakeAll();
  }
  signal(): void {
    if (Atomics.load(this.i32, NOTIFY * 2) !== 0) this.wakeAll();
  }
  wakeAll(): void {
    Atomics.add(this.i32, NOTIFY * 2 + 1, 1);
    Atomics.notify(this.i32, NOTIFY * 2 + 1);
  }
  /// Bounded 1 ms wait, so a missed signal only delays.
  block(): void {
    const v = Atomics.load(this.i32, NOTIFY * 2 + 1);
    Atomics.add(this.i32, NOTIFY * 2, 1);
    Atomics.wait(this.i32, NOTIFY * 2 + 1, v, 1);
    Atomics.sub(this.i32, NOTIFY * 2, 1);
  }
}

/// Allocates a ring's shared memory. `gating` defaults to the consumers no
/// other consumer depends on.
export function createRing(size: number, slotSize: number, kind: ProducerKind, deps: number[][],
  globalAlert?: SharedArrayBuffer): RingShared {
  if (size < 1 || (size & (size - 1)) !== 0) throw new Error("ring size must be a power of two");
  deps.forEach((d, i) => d.forEach((x) => { if (x >= i) throw new Error("declare dependencies first"); }));
  const depended = new Set(deps.flat());
  const gating = deps.map((_, i) => i).filter((i) => !depended.has(i));
  const headerBytes = (CONSUMERS + deps.length * W) * 8;
  const availOffset = headerBytes;
  const availBytes = kind === "multi" ? size * 4 : 0;
  const slotsOffset = Math.ceil((availOffset + availBytes) / 64) * 64;
  const sab = new SharedArrayBuffer(slotsOffset + size * slotSize);
  const sh: RingShared = { sab, size, slotSize, kind, consumers: deps.length, deps, gating, slotsOffset, availOffset, globalAlert };
  const h = new Header(sh);
  h.store(CURSOR, INITIAL);
  h.store(GATE_CACHE, INITIAL);
  for (let i = 0; i < deps.length; i++) h.store(CONSUMERS + i * W, INITIAL);
  if (kind === "multi") new Int32Array(sab, availOffset, size).fill(-1);
  return sh;
}

class Base {
  protected readonly h: Header;
  readonly view: DataView;
  readonly size: number;
  readonly mask: number;
  readonly shift: number;
  readonly slotSize: number;
  readonly slotsOffset: number;
  protected readonly avail?: Int32Array;
  protected readonly gating: number[];
  constructor(readonly sh: RingShared) {
    this.h = new Header(sh);
    this.view = new DataView(sh.sab);
    this.size = sh.size;
    this.mask = sh.size - 1;
    this.shift = Math.log2(sh.size);
    this.slotSize = sh.slotSize;
    this.slotsOffset = sh.slotsOffset;
    if (sh.kind === "multi") this.avail = new Int32Array(sh.sab, sh.availOffset, sh.size);
    this.gating = sh.gating.map((i) => CONSUMERS + i * W);
  }
  /// Byte offset of slot `seq` in `view`.
  offset(seq: number): number {
    return this.slotsOffset + (seq & this.mask) * this.slotSize;
  }
  minGating(): number {
    let m = Number.MAX_SAFE_INTEGER;
    for (const g of this.gating) m = Math.min(m, this.h.load(g));
    return m;
  }
  lap(seq: number): number {
    return Math.floor(seq / this.size) | 0;
  }
  isAvailable(seq: number): boolean {
    return Atomics.load(this.avail!, seq & this.mask) === this.lap(seq);
  }
  publishedUpto(lo: number, limit: number): number {
    const hi = Math.min(this.h.load(CURSOR), limit);
    if (this.avail === undefined) return hi;
    for (let q = lo; q <= hi; q++) if (!this.isAvailable(q)) return q - 1;
    return hi;
  }
  alert(): void {
    this.h.alert();
  }
  isAlerted(): boolean {
    return this.h.alerted();
  }
  /// Highest published sequence.
  published(): number {
    if (this.avail === undefined) return this.h.load(CURSOR);
    const floor = Math.min(this.minGating(), this.h.load(CURSOR));
    return this.publishedUpto(floor + 1, Number.MAX_SAFE_INTEGER);
  }
  consumed(): number {
    return this.minGating();
  }
}

function producerBackoff(step: { n: number }): void {
  if (step.n < 64) step.n++;
  else parkUs(1);
}

/// The only producer of a single ring: stage() + commit() publish a batch
/// with one store. stage returns the slot's byte offset in `view` (-1 once
/// alerted).
export class SingleProducer extends Base {
  private next: number;
  private publishedSeq: number;
  private cachedGate: number;
  private readonly step = { n: 0 };
  constructor(sh: RingShared) {
    super(sh);
    this.next = this.publishedSeq = this.cachedGate = this.h.load(CURSOR);
  }
  staged(): number {
    return this.next - this.publishedSeq;
  }
  hasRoom(n: number): boolean {
    const wrap = this.next + n - this.size;
    if (wrap <= this.cachedGate) return true;
    this.cachedGate = this.minGating();
    return wrap <= this.cachedGate;
  }
  /// Wait for room; `whileWaiting` runs between checks (the main thread pumps egress there).
  waitRoom(n: number, whileWaiting?: () => void): Publish {
    if (this.hasRoom(n)) return Publish.Ok;
    this.commit(); // consumers can't free space they can't see
    this.step.n = 0;
    while (!this.hasRoom(n)) {
      if (this.h.alerted()) return Publish.Alerted;
      if (whileWaiting) whileWaiting();
      producerBackoff(this.step);
    }
    return Publish.Ok;
  }
  stage(whileWaiting?: () => void): number {
    if (this.waitRoom(1, whileWaiting) !== Publish.Ok) return -1;
    return this.offset(++this.next);
  }
  commit(): void {
    if (this.next !== this.publishedSeq) {
      this.publishedSeq = this.next;
      this.h.store(CURSOR, this.next);
      this.h.signal();
    }
  }
  publish(fill: (view: DataView, off: number) => void): Publish {
    const off = this.stage();
    if (off < 0) return Publish.Alerted;
    fill(this.view, off);
    this.commit();
    return Publish.Ok;
  }
  tryPublish(fill: (view: DataView, off: number) => void): Publish {
    if (!this.hasRoom(1)) return Publish.Full;
    return this.publish(fill);
  }
  publishBatch(n: number, fill: (i: number, view: DataView, off: number) => void, whileWaiting?: () => void): Publish {
    if (this.waitRoom(n, whileWaiting) !== Publish.Ok) return Publish.Alerted;
    for (let i = 0; i < n; i++) fill(i, this.view, this.offset(++this.next));
    this.commit();
    return Publish.Ok;
  }
}

/// A producer of a multi ring; any number of workers may each attach one.
export class MultiProducer extends Base {
  private readonly step = { n: 0 };
  private roomFor(hi: number): boolean {
    const wrap = hi - this.size;
    if (wrap <= this.h.load(GATE_CACHE)) return true;
    const g = this.minGating();
    this.h.store(GATE_CACHE, g);
    return wrap <= g;
  }
  private fill(lo: number, hi: number, f: (i: number, view: DataView, off: number) => void): void {
    for (let q = lo; q <= hi; q++) f(q - lo, this.view, this.offset(q));
    for (let q = lo; q <= hi; q++) Atomics.store(this.avail!, q & this.mask, this.lap(q));
    this.h.signal();
  }
  publishBatch(n: number, f: (i: number, view: DataView, off: number) => void, whileWaiting?: () => void): Publish {
    const hi = Number(Atomics.add(this.h.seq, CURSOR, BigInt(n))) + n;
    this.step.n = 0;
    while (!this.roomFor(hi)) {
      if (this.h.alerted()) return Publish.Alerted;
      if (whileWaiting) whileWaiting();
      producerBackoff(this.step);
    }
    this.fill(hi - n + 1, hi, f);
    return Publish.Ok;
  }
  publish(f: (view: DataView, off: number) => void): Publish {
    return this.publishBatch(1, (_i, v, o) => f(v, o));
  }
  /// Claims by CAS only if it fits: a fetch-add claim could not be backed out.
  tryPublishBatch(n: number, f: (i: number, view: DataView, off: number) => void): Publish {
    for (;;) {
      if (this.h.alerted()) return Publish.Alerted;
      const cur = this.h.load(CURSOR);
      const hi = cur + n;
      if (!this.roomFor(hi)) return Publish.Full;
      if (Atomics.compareExchange(this.h.seq, CURSOR, BigInt(cur), BigInt(hi)) === BigInt(cur)) {
        this.fill(cur + 1, hi, f);
        return Publish.Ok;
      }
    }
  }
  tryPublish(f: (view: DataView, off: number) => void): Publish {
    return this.tryPublishBatch(1, (_i, v, o) => f(v, o));
  }
}

export type Handler = (view: DataView, off: number, seq: number, endOfBatch: boolean) => void;

/// A read-only view of a ring, for statistics (depth = published - consumed).
export class RingView extends Base {}

/// One consumer: a barrier (cursor + upstream consumers) plus its own watermark.
export class Consumer extends Base {
  private nextSeq: number;
  private maxBatch = 1024;
  private readonly deps: number[];
  private readonly mine: number;
  private step = 0;
  private park = 0;
  constructor(sh: RingShared, readonly index: number, private readonly wait: WaitStrategy = backoff()) {
    super(sh);
    this.mine = CONSUMERS + index * W;
    this.deps = sh.deps[index].map((d) => CONSUMERS + d * W);
    this.nextSeq = this.h.load(this.mine) + 1;
  }
  sequence(): number {
    return this.h.load(this.mine);
  }
  setMaxBatch(n: number): void {
    this.maxBatch = n;
  }
  available(): number {
    const limit = this.nextSeq + this.maxBatch - 1;
    if (this.deps.length === 0) return this.publishedUpto(this.nextSeq, limit);
    let m = limit;
    for (const d of this.deps) m = Math.min(m, this.h.load(d));
    return m;
  }
  /// Everything available (up to the batch cap), without waiting.
  poll(h: Handler): number {
    const avail = this.available();
    if (avail < this.nextSeq) return 0;
    for (let q = this.nextSeq; q <= avail; q++) h(this.view, this.offset(q), q, q === avail);
    const n = avail - this.nextSeq + 1;
    this.nextSeq = avail + 1;
    this.h.store(this.mine, avail);
    this.h.signal();
    return n;
  }
  /// Wait for at least one event, then poll; false once alerted and empty.
  waitPoll(h: Handler): boolean {
    for (;;) {
      if (this.poll(h) > 0) {
        this.resetIdle();
        return true;
      }
      if (this.h.alerted()) return false;
      this.idle();
    }
  }
  resetIdle(): void {
    this.step = 0;
  }
  idle(): void {
    const s = this.wait;
    switch (s.kind) {
      case "busySpin":
        return;
      case "yield":
        if (this.step < s.spin) this.step++;
        else parkUs(1);
        return;
      case "backoff":
        if (this.step < s.spin + s.yields) {
          this.step++;
          if (this.step > s.spin) parkUs(1);
          return;
        }
        if (this.step === s.spin + s.yields) {
          this.step++;
          this.park = s.parkMinUs;
        }
        parkUs(this.park);
        this.park = Math.min(this.park * 2, s.parkMaxUs);
        return;
      case "blocking":
        if (this.step < s.spin) this.step++;
        else this.h.block();
        return;
    }
  }
}
