//! Publishing from any thread.
//!
//! The ingress ring is multi-producer: every publisher claims slots with an
//! atomic add. A `Handle` is built from a `HandleDescriptor`, a plain object
//! a pipeline hands out (`Pipeline.handleDescriptor()`) and that can be sent
//! to a worker through `workerData`. Each handle owns an in-flight flag in
//! shared memory, so shutdown can wait out publishes that began before it
//! closed the pipeline: a publish that returns Ok is always applied
//! (spec/PIPELINE.md §6), whichever thread made it.

import { Command } from "./matcher/types";
import { writeCmd } from "./msg";
import { MultiProducer, Publish, RingShared } from "./ring";

/// Handles one pipeline can have at once (the owner's included).
export const MAX_HANDLES = 64;
// control words: [0] closed, [1] timestamps on, [2 + i] in-flight, [2 + MAX + i] slot taken
const CLOSED = 0, STAMPS = 1, FLIGHT = 2, TAKEN = 2 + MAX_HANDLES;
export const HANDLE_CTL_WORDS = 2 + 2 * MAX_HANDLES;

/// Publish outcome: Ok means sequenced and will be applied — not durable.
export enum Status {
  Ok,
  Closed,
  Full,
}

/// Everything a thread needs to publish into a pipeline.
export interface HandleDescriptor {
  ingress: RingShared;
  ctl: SharedArrayBuffer;
  /// The pipeline's epoch, in absolute ms (performance.timeOrigin + now()).
  epochAbsMs: number;
  slot: number;
}

/// Take a free handle slot in `ctl`; -1 when all are in use.
export function takeSlot(ctl: SharedArrayBuffer): number {
  const w = new Int32Array(ctl);
  for (let i = 0; i < MAX_HANDLES; i++) if (Atomics.compareExchange(w, TAKEN + i, 0, 1) === 0) return i;
  return -1;
}

export function setClosed(ctl: SharedArrayBuffer): void {
  Atomics.store(new Int32Array(ctl), CLOSED, 1);
}

export function setStamps(ctl: SharedArrayBuffer, on: boolean): void {
  Atomics.store(new Int32Array(ctl), STAMPS, on ? 1 : 0);
}

/// Is any handle mid-publish?
export function anyInFlight(ctl: SharedArrayBuffer): boolean {
  const w = new Int32Array(ctl);
  for (let i = 0; i < MAX_HANDLES; i++) if (Atomics.load(w, FLIGHT + i) !== 0) return true;
  return false;
}

export class Handle {
  private readonly w: Int32Array;
  private readonly ingress: MultiProducer;
  private readonly slot: number;
  private readonly epochAbs: number;
  private released = false;

  /// `whileWaiting` runs while the ingress is full (the owner pumps egress there).
  constructor(d: HandleDescriptor, private readonly whileWaiting?: () => void) {
    this.w = new Int32Array(d.ctl);
    this.ingress = new MultiProducer(d.ingress);
    this.slot = d.slot;
    this.epochAbs = d.epochAbsMs;
  }

  private now(): number {
    if (Atomics.load(this.w, STAMPS) === 0) return 0;
    return Math.max(Math.round((performance.timeOrigin + performance.now() - this.epochAbs) * 1e6), 1);
  }

  /// Raise the in-flight flag, then check closed (Atomics are sequentially
  /// consistent: the store is ordered before the load).
  private enter(): boolean {
    if (this.released) return false;
    Atomics.store(this.w, FLIGHT + this.slot, 1);
    if (Atomics.load(this.w, CLOSED) !== 0) {
      Atomics.store(this.w, FLIGHT + this.slot, 0);
      return false;
    }
    return true;
  }

  private exit(): void {
    Atomics.store(this.w, FLIGHT + this.slot, 0);
  }

  /// Sequence one command (waits while ingress is full).
  publish(sym: number, cmd: Command): Status {
    if (!this.enter()) return Status.Closed;
    const t = this.now();
    const r = this.ingress.publishBatch(1, (_i, v, o) => writeCmd(v, o, sym, cmd, t), this.whileWaiting);
    this.exit();
    return r === Publish.Ok ? Status.Ok : Status.Closed;
  }

  /// Sequence one command, or Full without waiting.
  tryPublish(sym: number, cmd: Command): Status {
    if (!this.enter()) return Status.Closed;
    const t = this.now();
    const r = this.ingress.tryPublish((v, o) => writeCmd(v, o, sym, cmd, t));
    this.exit();
    return r === Publish.Ok ? Status.Ok : r === Publish.Full ? Status.Full : Status.Closed;
  }

  /// Many commands, one claim per chunk (consecutive iseqs within a chunk).
  publishBatch(cmds: ReadonlyArray<readonly [number, Command]>, from = 0, to = cmds.length): Status {
    if (!this.enter()) return Status.Closed;
    const chunk = Math.min(this.ingress.size, 256);
    let r = Publish.Ok;
    for (let off = from; off < to && r === Publish.Ok; off += chunk) {
      const k = Math.min(chunk, to - off), t = this.now();
      r = this.ingress.publishBatch(k, (i, v, o) => writeCmd(v, o, cmds[off + i][0], cmds[off + i][1], t), this.whileWaiting);
    }
    this.exit();
    return r === Publish.Ok ? Status.Ok : Status.Closed;
  }

  /// Give the slot back; the handle publishes nothing afterwards.
  close(): void {
    if (this.released) return;
    this.released = true;
    Atomics.store(this.w, TAKEN + this.slot, 0);
  }
}
