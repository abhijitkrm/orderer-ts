//! The MatchingCore seam (orderer-rust orderer-core/src/core.rs): what a
//! partition's engine worker needs from a matching core — plus strict
//! snapshot parsing and restore validation.

import { BookConfig, OrderBook, RestingOrder } from "./matcher/book";
import { IndexKind } from "./matcher/priceindex";
import { Sink } from "./matcher/sink";
import { writeBook } from "./matcher/snapshot";
import { CloseReason, Command, Event, Side } from "./matcher/types";
import { get, i64, indexName, parseHeader, parseTif, u64 } from "./flat";

/// emit(symbol, seq, event) for every event, in match order.
export type Emit = (sym: number, seq: number, ev: Event) => void;

/// One snapshot book block (matcher-snap/1 lines, no header, newline-terminated).
export interface Block {
  symbol: number;
  text: string;
}

export interface MatchingCore {
  apply(sym: number, cmd: Command, emit: Emit): void;
  /// This core's book blocks, any order (the pipeline merges by symbol).
  snapshotBlocks(out: Block[]): void;
  /// Install one book from a snapshot block; error text, or undefined.
  restoreBook(sym: number, seq: number, orders: RestingOrder[]): string | undefined;
}

export type CoreFactory = (cfg: BookConfig) => MatchingCore;

/// Can `orders` be restored as `sym`'s book under `cfg`?
export function validateBook(cfg: BookConfig, sym: number, orders: RestingOrder[]): string | undefined {
  const pre = `snapshot book ${sym}: `;
  if (orders.length > cfg.maxOrders) return `${pre}${orders.length} orders exceed max_orders ${cfg.maxOrders}`;
  const ids = new Set<number>();
  for (const o of orders) {
    if (ids.has(o.orderId)) return pre + "duplicate order_id";
    ids.add(o.orderId);
  }
  let bestBid: number | undefined, bestAsk: number | undefined;
  for (const o of orders) {
    if (o.qty === 0) return `${pre}order ${o.orderId} has qty 0`;
    const ok = cfg.index === IndexKind.Ladder ? o.price >= cfg.priceMin && o.price <= cfg.priceMax : o.price > 0;
    if (!ok) return `${pre}order ${o.orderId} price out of range`;
    if (o.side === Side.Bid) bestBid = bestBid === undefined ? o.price : Math.max(bestBid, o.price);
    else bestAsk = bestAsk === undefined ? o.price : Math.min(bestAsk, o.price);
  }
  if (bestBid !== undefined && bestAsk !== undefined && bestBid >= bestAsk) return pre + "crossed book";
  return undefined;
}

/// The spec-proven FIFO core: matcher-ts's OrderBook, one per symbol, with
/// a reused tagging sink.
export class FifoCore implements MatchingCore {
  private readonly dense: Array<OrderBook | undefined> = new Array(4096).fill(undefined);
  private readonly sparse = new Map<number, OrderBook>();
  private sym = 0;
  private emit: Emit = () => {};
  private readonly tag: Sink = { onEvent: (seq, ev) => this.emit(this.sym, seq, ev) };

  constructor(private readonly cfg: BookConfig) {}

  private book(sym: number): OrderBook {
    if (sym < 4096) {
      let b = this.dense[sym];
      if (b === undefined) this.dense[sym] = b = new OrderBook(this.cfg);
      return b;
    }
    let b = this.sparse.get(sym);
    if (b === undefined) this.sparse.set(sym, (b = new OrderBook(this.cfg)));
    return b;
  }

  apply(sym: number, cmd: Command, emit: Emit): void {
    this.sym = sym;
    this.emit = emit;
    this.book(sym).apply(cmd, this.tag);
  }

  snapshotBlocks(out: Block[]): void {
    const block = (s: number, b: OrderBook) => {
      const lines: string[] = [];
      writeBook(b, s, lines);
      out.push({ symbol: s, text: lines.join("\n") + "\n" });
    };
    this.dense.forEach((b, s) => b !== undefined && block(s, b));
    for (const [s, b] of this.sparse) block(s, b);
  }

  restoreBook(sym: number, seq: number, orders: RestingOrder[]): string | undefined {
    const err = validateBook(this.cfg, sym, orders);
    if (err !== undefined) return err;
    const b = OrderBook.restore(this.cfg, seq, orders);
    if (sym < 4096) this.dense[sym] = b;
    else this.sparse.set(sym, b);
    return undefined;
  }
}

/// Test core: echoes each command as one event with a dense per-symbol seq.
export class NoopCore implements MatchingCore {
  private readonly seqs = new Map<number, number>();
  constructor(_cfg: BookConfig) {}
  apply(sym: number, cmd: Command, emit: Emit): void {
    const s = (this.seqs.get(sym) ?? 0) + 1;
    this.seqs.set(sym, s);
    const ev: Event =
      cmd.kind === "new" ? { kind: "accepted", orderId: cmd.orderId, leavesQty: cmd.qty }
      : cmd.kind === "cancel" ? { kind: "closed", orderId: cmd.orderId, reason: CloseReason.Cancelled }
      : { kind: "replaced", orderId: cmd.orderId, price: cmd.price, qty: cmd.qty };
    emit(sym, s, ev);
  }
  snapshotBlocks(out: Block[]): void {
    for (const [s, q] of this.seqs) out.push({ symbol: s, text: `{"rec":"book","symbol":${s},"seq":${q}}\n` });
  }
  restoreBook(sym: number, seq: number): string | undefined {
    this.seqs.set(sym, seq);
    return undefined;
  }
}

/// Built-in cores by name; a worker resolves "module#export" for custom ones.
export const CORES: Record<string, CoreFactory> = {
  fifo: (c) => new FifoCore(c),
  noop: (c) => new NoopCore(c),
};

export function resolveCore(spec: string): CoreFactory {
  const builtin = CORES[spec];
  if (builtin !== undefined) return builtin;
  const [mod, name] = spec.split("#");
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const f = require(mod)[name ?? "default"];
  if (typeof f !== "function") throw new Error(`core ${spec}: no factory export`);
  return f as CoreFactory;
}

// ---- matcher-snap/1, strictly ---------------------------------------------------------------

export interface SnapBook {
  symbol: number;
  seq: number;
  orders: RestingOrder[];
}

export interface ParsedSnapshot {
  cfg: BookConfig;
  books: SnapBook[];
}

export function snapshotHeader(c: BookConfig): string {
  return `{"format":"matcher-snap/1","pmin":${c.priceMin},"pmax":${c.priceMax},"max_orders":${c.maxOrders},"index":"${indexName(c.index)}"}\n`;
}

/// Strict parse (orderer-rust try_parse): malformed input throws.
export function parseSnapshot(text: string): ParsedSnapshot {
  const lines = text.split("\n");
  if (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
  if (text === "" || lines.length === 0) throw new Error("snapshot line 1: empty snapshot");
  if (get(lines[0], "format") !== "matcher-snap/1") throw new Error("snapshot line 1: not a matcher-snap/1 header");
  const ps: ParsedSnapshot = { cfg: parseHeader(lines[0]), books: [] };
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (line === "") continue;
    const pre = `snapshot line ${i + 1}: `;
    const rec = get(line, "rec");
    if (rec === "book") {
      ps.books.push({ symbol: (u64(line, "symbol") ?? 0) >>> 0, seq: u64(line, "seq") ?? 0, orders: [] });
    } else if (rec === "order") {
      const orderId = u64(line, "order_id");
      if (orderId === undefined) throw new Error(pre + "bad order_id");
      const side = get(line, "side");
      if (side !== "bid" && side !== "ask") throw new Error(pre + "bad side");
      const tif = parseTif(get(line, "tif"));
      if (tif === undefined) throw new Error(pre + "bad tif");
      const price = i64(line, "price");
      if (price === undefined) throw new Error(pre + "bad price");
      const qty = u64(line, "qty");
      if (qty === undefined) throw new Error(pre + "bad qty");
      if (ps.books.length === 0) throw new Error(pre + "order line before book block");
      ps.books[ps.books.length - 1].orders.push({ orderId, side: side === "ask" ? Side.Ask : Side.Bid, price, qty, tif });
    } else {
      throw new Error(pre + "bad rec");
    }
  }
  return ps;
}
