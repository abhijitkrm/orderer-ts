//! Core types shared across the matcher. Field encodings and canonical
//! serialization follow spec/SPEC.md + spec/SCHEMA.md.

export type OrderId = number;
export type Symbol = number;
export type Price = number;
export type Qty = number;

export enum Side {
  Bid,
  Ask,
}

export function sideStr(s: Side): string {
  return s === Side.Bid ? "bid" : "ask";
}

export function sideFromStr(s: string | undefined): Side | undefined {
  return s === "bid" ? Side.Bid : s === "ask" ? Side.Ask : undefined;
}

export enum OType {
  Limit,
  Market,
}

export function otypeStr(t: OType): string {
  return t === OType.Limit ? "limit" : "market";
}

export enum Tif {
  Gtc,
  Ioc,
  Fok,
  PostOnly,
}

export function tifStr(t: Tif): string {
  switch (t) {
    case Tif.Gtc: return "gtc";
    case Tif.Ioc: return "ioc";
    case Tif.Fok: return "fok";
    case Tif.PostOnly: return "post_only";
  }
}

export enum RejectReason {
  InvalidQty,
  InvalidPrice,
  DuplicateOrderId,
  UnknownOrderId,
  PostOnlyWouldCross,
  FokCannotFill,
  BookFull,
}

export function rejectStr(r: RejectReason): string {
  switch (r) {
    case RejectReason.InvalidQty: return "invalid_qty";
    case RejectReason.InvalidPrice: return "invalid_price";
    case RejectReason.DuplicateOrderId: return "duplicate_order_id";
    case RejectReason.UnknownOrderId: return "unknown_order_id";
    case RejectReason.PostOnlyWouldCross: return "post_only_would_cross";
    case RejectReason.FokCannotFill: return "fok_cannot_fill";
    case RejectReason.BookFull: return "book_full";
  }
}

export enum CloseReason {
  Filled,
  Cancelled,
  Expired,
}

export function closeStr(r: CloseReason): string {
  switch (r) {
    case CloseReason.Filled: return "filled";
    case CloseReason.Cancelled: return "cancelled";
    case CloseReason.Expired: return "expired";
  }
}

/// A command submitted to a book.
export type Command =
  | { kind: "new"; orderId: OrderId; side: Side; otype: OType; price: Price; qty: Qty; tif: Tif }
  | { kind: "cancel"; orderId: OrderId }
  | { kind: "replace"; orderId: OrderId; price: Price; qty: Qty };

export function newLimit(orderId: OrderId, side: Side, price: Price, qty: Qty, tif: Tif): Command {
  return { kind: "new", orderId, side, otype: OType.Limit, price, qty, tif };
}

export function newMarket(orderId: OrderId, side: Side, qty: Qty): Command {
  return { kind: "new", orderId, side, otype: OType.Market, price: 0, qty, tif: Tif.Ioc };
}

export function cancel(orderId: OrderId): Command {
  return { kind: "cancel", orderId };
}

export function replace(orderId: OrderId, price: Price, qty: Qty): Command {
  return { kind: "replace", orderId, price, qty };
}

/// An event emitted by a book, paired with a per-book `seq` at emit time.
export type Event =
  | { kind: "accepted"; orderId: OrderId; leavesQty: Qty }
  | { kind: "rejected"; orderId: OrderId; reason: RejectReason }
  | { kind: "trade"; maker: OrderId; taker: OrderId; price: Price; qty: Qty }
  | { kind: "closed"; orderId: OrderId; reason: CloseReason }
  | { kind: "replaced"; orderId: OrderId; price: Price; qty: Qty };

/// Canonical event line (SCHEMA.md) — no trailing newline. `sym` adds the
/// `"symbol":N` field used by `engine:true` vectors.
export function eventCanonical(seq: number, ev: Event, sym?: Symbol): string {
  const sf = sym === undefined ? "" : `,"symbol":${sym}`;
  switch (ev.kind) {
    case "accepted":
      return `{"seq":${seq},"ev":"accepted"${sf},"order_id":${ev.orderId},"leaves_qty":${ev.leavesQty}}`;
    case "rejected":
      return `{"seq":${seq},"ev":"rejected"${sf},"order_id":${ev.orderId},"reason":"${rejectStr(ev.reason)}"}`;
    case "trade":
      return `{"seq":${seq},"ev":"trade"${sf},"maker":${ev.maker},"taker":${ev.taker},"price":${ev.price},"qty":${ev.qty}}`;
    case "closed":
      return `{"seq":${seq},"ev":"closed"${sf},"order_id":${ev.orderId},"reason":"${closeStr(ev.reason)}"}`;
    case "replaced":
      return `{"seq":${seq},"ev":"replaced"${sf},"order_id":${ev.orderId},"price":${ev.price},"qty":${ev.qty}}`;
  }
}

/// Deterministic fold over event fields — used by NullSink to keep the event
/// stream "consumed" for benchmarking without storing it.
export function eventFold(ev: Event): number {
  const M = 0x9e3779b1;
  const mix = (a: number, b: number) => (Math.imul(a, M) + b) | 0;
  switch (ev.kind) {
    case "accepted": return mix(ev.orderId, ev.leavesQty);
    case "rejected": return mix(ev.orderId, ev.reason);
    case "trade": return mix(mix(ev.maker, ev.taker), mix(ev.price, ev.qty));
    case "closed": return mix(ev.orderId, ev.reason);
    case "replaced": return mix(ev.orderId, mix(ev.price, ev.qty));
  }
}
