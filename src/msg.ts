//! Ring slot layouts (implementation-private) and their codecs. Numbers are
//! float64 (exact to 2^53, matcher-ts's range); little-endian throughout.

import { Command, Event, OType, Side, Tif } from "./matcher/types";

/// Control operations ride the rings so they cut every partition at the
/// same point of the ingress order (spec/PIPELINE.md §6).
export enum Control {
  None,
  Barrier,
  Snapshot,
  Shutdown,
  Checkpoint,
}

// ---- CmdMsg: ingress / inbox slot (64 bytes) ----------------------------------------------
//  0 iseq · 8 tPub · 16 arg · 24 orderId · 32 price · 40 qty   (f64)
// 48 symbol (u32) · 52 ctl · 53 kind (0 new, 1 cancel, 2 replace) · 54 side · 55 otype · 56 tif
export const CMD_SLOT = 64;

export function writeCmd(v: DataView, o: number, sym: number, c: Command, tPub: number): void {
  v.setFloat64(o + 8, tPub, true);
  v.setFloat64(o + 16, 0, true);
  v.setFloat64(o + 24, c.orderId, true);
  v.setUint32(o + 48, sym, true);
  v.setUint8(o + 52, Control.None);
  if (c.kind === "new") {
    v.setFloat64(o + 32, c.price, true);
    v.setFloat64(o + 40, c.qty, true);
    v.setUint8(o + 53, 0);
    v.setUint8(o + 54, c.side);
    v.setUint8(o + 55, c.otype);
    v.setUint8(o + 56, c.tif);
  } else if (c.kind === "cancel") {
    v.setFloat64(o + 32, 0, true);
    v.setFloat64(o + 40, 0, true);
    v.setUint8(o + 53, 1);
  } else {
    v.setFloat64(o + 32, c.price, true);
    v.setFloat64(o + 40, c.qty, true);
    v.setUint8(o + 53, 2);
  }
}

export function writeCtl(v: DataView, o: number, ctl: Control, arg: number): void {
  v.setFloat64(o + 8, 0, true);
  v.setFloat64(o + 16, arg, true);
  v.setUint32(o + 48, 0, true);
  v.setUint8(o + 52, ctl);
}

export const cmdIseq = (v: DataView, o: number) => v.getFloat64(o, true);
export const setCmdIseq = (v: DataView, o: number, iseq: number) => v.setFloat64(o, iseq, true);
export const cmdTPub = (v: DataView, o: number) => v.getFloat64(o + 8, true);
export const cmdArg = (v: DataView, o: number) => v.getFloat64(o + 16, true);
export const cmdSym = (v: DataView, o: number) => v.getUint32(o + 48, true);
export const cmdCtl = (v: DataView, o: number): Control => v.getUint8(o + 52);

export function readCmd(v: DataView, o: number): Command {
  const orderId = v.getFloat64(o + 24, true);
  switch (v.getUint8(o + 53)) {
    case 0:
      return {
        kind: "new", orderId, side: v.getUint8(o + 54) as Side, otype: v.getUint8(o + 55) as OType,
        price: v.getFloat64(o + 32, true), qty: v.getFloat64(o + 40, true), tif: v.getUint8(o + 56) as Tif,
      };
    case 1:
      return { kind: "cancel", orderId };
    default:
      return { kind: "replace", orderId, price: v.getFloat64(o + 32, true), qty: v.getFloat64(o + 40, true) };
  }
}

/// Copy one command slot (router: ingress → inbox), field by field: no allocation.
export function copyCmd(from: DataView, fo: number, to: DataView, too: number): void {
  for (let i = 0; i < 48; i += 8) to.setFloat64(too + i, from.getFloat64(fo + i, true), true);
  to.setUint32(too + 48, from.getUint32(fo + 48, true), true);
  to.setUint32(too + 52, from.getUint32(fo + 52, true), true);
  to.setUint8(too + 56, from.getUint8(fo + 56));
}

// ---- EvtMsg: outbox slot (72 bytes) ----------------------------------------------------------
//  0 iseq · 8 seq · 16 tPub · 24 arg · 32 a · 40 b · 48 c · 56 d   (f64)
// 64 symbol (u32) · 68 ctl · 69 ev (0 accepted … 4 replaced) · 70 reason
export const EVT_SLOT = 72;

/// An event as egress plugs see it. Plugs receive one reused view per
/// partition (the ring slot, decoded on demand), as Rust plugs borrow the
/// slot: copy what you keep (`copyEvtMsg`).
export interface EvtMsg {
  readonly iseq: number;
  readonly seq: number;
  readonly tPub: number;
  readonly symbol: number;
  /// The event's kind, without decoding the rest.
  readonly kind: Event["kind"];
  /// The decoded event (built on first access per slot).
  readonly ev: Event;
}

const KINDS: Array<Event["kind"]> = ["accepted", "rejected", "trade", "closed", "replaced"];

/// A reusable EvtMsg over the outbox slot at `off`.
export class EvtView implements EvtMsg {
  iseq = 0;
  seq = 0;
  tPub = 0;
  symbol = 0;
  private off = 0;
  private decoded: Event | undefined;
  constructor(private readonly v: DataView) {}
  at(off: number): this {
    const v = this.v;
    this.off = off;
    this.iseq = v.getFloat64(off, true);
    this.seq = v.getFloat64(off + 8, true);
    this.tPub = v.getFloat64(off + 16, true);
    this.symbol = v.getUint32(off + 64, true);
    this.decoded = undefined;
    return this;
  }
  get kind(): Event["kind"] {
    return KINDS[this.v.getUint8(this.off + 69)];
  }
  get ev(): Event {
    return (this.decoded ??= readEvent(this.v, this.off));
  }
}

/// A detached copy of an EvtMsg (for plugs that hold events).
export function copyEvtMsg(m: EvtMsg): EvtMsg {
  return { iseq: m.iseq, seq: m.seq, tPub: m.tPub, symbol: m.symbol, kind: m.kind, ev: m.ev };
}

export function writeEvt(v: DataView, o: number, iseq: number, seq: number, tPub: number, sym: number, e: Event): void {
  v.setFloat64(o, iseq, true);
  v.setFloat64(o + 8, seq, true);
  v.setFloat64(o + 16, tPub, true);
  v.setUint32(o + 64, sym, true);
  v.setUint8(o + 68, Control.None);
  let a = 0, b = 0, c = 0, d = 0, kind = 0, reason = 0;
  switch (e.kind) {
    case "accepted": a = e.orderId; d = e.leavesQty; break;
    case "rejected": kind = 1; a = e.orderId; reason = e.reason; break;
    case "trade": kind = 2; a = e.maker; b = e.taker; c = e.price; d = e.qty; break;
    case "closed": kind = 3; a = e.orderId; reason = e.reason; break;
    case "replaced": kind = 4; a = e.orderId; c = e.price; d = e.qty; break;
  }
  v.setFloat64(o + 32, a, true);
  v.setFloat64(o + 40, b, true);
  v.setFloat64(o + 48, c, true);
  v.setFloat64(o + 56, d, true);
  v.setUint8(o + 69, kind);
  v.setUint8(o + 70, reason);
}

export function writeEvtCtl(v: DataView, o: number, iseq: number, ctl: Control, arg: number): void {
  v.setFloat64(o, iseq, true);
  v.setFloat64(o + 24, arg, true);
  v.setUint8(o + 68, ctl);
}

export const evtCtl = (v: DataView, o: number): Control => v.getUint8(o + 68);
export const evtIseq = (v: DataView, o: number) => v.getFloat64(o, true);
export const evtArg = (v: DataView, o: number) => v.getFloat64(o + 24, true);

export function readEvent(v: DataView, o: number): Event {
  const a = v.getFloat64(o + 32, true), reason = v.getUint8(o + 70);
  switch (v.getUint8(o + 69)) {
    case 0: return { kind: "accepted", orderId: a, leavesQty: v.getFloat64(o + 56, true) };
    case 1: return { kind: "rejected", orderId: a, reason };
    case 2: return { kind: "trade", maker: a, taker: v.getFloat64(o + 40, true), price: v.getFloat64(o + 48, true), qty: v.getFloat64(o + 56, true) };
    case 3: return { kind: "closed", orderId: a, reason };
    default: return { kind: "replaced", orderId: a, price: v.getFloat64(o + 48, true), qty: v.getFloat64(o + 56, true) };
  }
}


