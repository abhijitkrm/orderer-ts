//! The matching core. Single-writer, deterministic: commands in through
//! `apply`, sequenced events out through the Sink seam. Mirrors spec/SPEC.md.

import { NIL, Pool } from "./pool";
import { OrderMap } from "./ordermap";
import { IndexKind, PriceIndex } from "./priceindex";
import {
  CloseReason,
  Command,
  Event,
  OType,
  OrderId,
  Price,
  Qty,
  RejectReason,
  Side,
  Tif,
} from "./types";
import { Sink } from "./sink";

export interface BookConfig {
  priceMin: Price;
  priceMax: Price;
  maxOrders: number;
  index: IndexKind;
}

export const defaultConfig = (): BookConfig => ({
  priceMin: 0,
  priceMax: 1_000_000,
  maxOrders: 65_536,
  index: IndexKind.Ladder,
});

export interface OrderInfo {
  id: OrderId;
  side: Side;
  price: Price;
  qty: Qty;
}

/// One live order, for snapshot serialization (spec/JOURNAL.md).
export interface RestingOrder {
  orderId: OrderId;
  side: Side;
  price: Price;
  qty: Qty;
  tif: Tif;
}

export class OrderBook {
  private readonly pool: Pool;
  private readonly map: OrderMap;
  private readonly bids: PriceIndex;
  private readonly asks: PriceIndex;
  private seq = 0;
  private readonly cfg: BookConfig;

  constructor(cfg: BookConfig) {
    if (!(cfg.priceMax > cfg.priceMin) && cfg.index !== IndexKind.Tree) {
      throw new Error("price range must be non-empty");
    }
    this.pool = new Pool(cfg.maxOrders);
    this.map = new OrderMap(cfg.maxOrders);
    const mk = (side: Side) =>
      cfg.index === IndexKind.Ladder
        ? PriceIndex.ladder(side, cfg.priceMin, cfg.priceMax)
        : PriceIndex.tree(side);
    this.bids = mk(Side.Bid);
    this.asks = mk(Side.Ask);
    this.cfg = cfg;
  }

  /// Apply one command, emitting its event stream through `sink`.
  apply(cmd: Command, sink: Sink): void {
    switch (cmd.kind) {
      case "new":
        this.newOrder(cmd.orderId, cmd.side, cmd.otype, cmd.price, cmd.qty, cmd.tif, sink);
        break;
      case "cancel":
        this.cancel(cmd.orderId, sink);
        break;
      case "replace":
        this.replace(cmd.orderId, cmd.price, cmd.qty, sink);
        break;
    }
  }

  // ---- commands -----------------------------------------------------------

  private newOrder(
    orderId: OrderId,
    side: Side,
    otype: OType,
    price: Price,
    qty: Qty,
    tif: Tif,
    sink: Sink,
  ): void {
    // SPEC §4.3 validation precedence.
    if (qty === 0) {
      return this.reject(sink, orderId, RejectReason.InvalidQty);
    }
    if (otype === OType.Limit && !this.priceOk(price)) {
      return this.reject(sink, orderId, RejectReason.InvalidPrice);
    }
    if (this.map.contains(orderId)) {
      return this.reject(sink, orderId, RejectReason.DuplicateOrderId);
    }
    if (this.pool.live >= this.cfg.maxOrders) {
      return this.reject(sink, orderId, RejectReason.BookFull);
    }
    if (otype === OType.Limit) {
      if (tif === Tif.PostOnly) {
        if (this.wouldCross(side, price)) {
          return this.reject(sink, orderId, RejectReason.PostOnlyWouldCross);
        }
      } else if (tif === Tif.Fok && this.fillable(side, price) < qty) {
        return this.reject(sink, orderId, RejectReason.FokCannotFill);
      }
    }

    const bound = otype === OType.Limit ? price : undefined;
    const remaining = this.cross(side, bound, orderId, qty, sink);

    if (remaining === 0) {
      this.emit(sink, { kind: "closed", orderId, reason: CloseReason.Filled });
    } else if (otype === OType.Limit && (tif === Tif.Gtc || tif === Tif.PostOnly)) {
      this.rest(orderId, side, price, remaining, tif);
      this.emit(sink, { kind: "accepted", orderId, leavesQty: remaining });
    } else {
      this.emit(sink, { kind: "closed", orderId, reason: CloseReason.Expired });
    }
  }

  private cancel(orderId: OrderId, sink: Sink): void {
    const idx = this.map.get(orderId);
    if (idx < 0) {
      return this.reject(sink, orderId, RejectReason.UnknownOrderId);
    }
    const price = this.pool.price[idx];
    const side = this.pool.side[idx];
    const own = side === Side.Bid ? this.bids : this.asks;
    const lvl = own.levelMut(price);
    if (lvl !== undefined) this.pool.levelUnlink(lvl, idx);
    own.unlinkLevel(price);
    this.map.remove(orderId);
    this.pool.free(idx);
    this.emit(sink, { kind: "closed", orderId, reason: CloseReason.Cancelled });
  }

  private replace(orderId: OrderId, price: Price, qty: Qty, sink: Sink): void {
    // SPEC §4.3: unknown -> invalid_qty -> invalid_price.
    const idx = this.map.get(orderId);
    if (idx < 0) {
      return this.reject(sink, orderId, RejectReason.UnknownOrderId);
    }
    if (qty === 0) {
      return this.reject(sink, orderId, RejectReason.InvalidQty);
    }
    if (!this.priceOk(price)) {
      return this.reject(sink, orderId, RejectReason.InvalidPrice);
    }
    const oldPrice = this.pool.price[idx];
    const oldQty = this.pool.qty[idx];
    const side = this.pool.side[idx];
    const own = side === Side.Bid ? this.bids : this.asks;

    if (price === oldPrice && qty <= oldQty) {
      // Quantity decrease (or no-op): keeps time priority.
      const lvl = own.levelMut(price);
      if (lvl !== undefined) lvl.total -= oldQty - qty;
      this.pool.qty[idx] = qty;
      this.emit(sink, { kind: "replaced", orderId, price, qty });
      return;
    }

    // Priority loss: unlink and re-enter the aggressive GTC limit path.
    const oldLvl = own.levelMut(oldPrice);
    if (oldLvl !== undefined) this.pool.levelUnlink(oldLvl, idx);
    own.unlinkLevel(oldPrice);
    this.pool.price[idx] = price;
    this.pool.qty[idx] = qty;

    const remaining = this.cross(side, price, orderId, qty, sink);

    if (remaining === 0) {
      this.map.remove(orderId);
      this.pool.free(idx);
      this.emit(sink, { kind: "closed", orderId, reason: CloseReason.Filled });
    } else {
      this.pool.qty[idx] = remaining;
      const lvl = own.levelInsert(price);
      lvl.total += remaining;
      this.pool.levelPush(lvl, idx);
      this.emit(sink, { kind: "replaced", orderId, price, qty: remaining });
    }
  }

  // ---- matching core ------------------------------------------------------

  /// Aggressive walk of the opposite side. `bound === undefined` means market
  /// (match all available depth). Returns what is left of `qty`.
  private cross(
    side: Side,
    bound: Price | undefined,
    taker: OrderId,
    qty: Qty,
    sink: Sink,
  ): Qty {
    const pool = this.pool;
    const map = this.map;
    const opp = side === Side.Bid ? this.asks : this.bids;

    for (;;) {
      const bp = opp.bestPrice();
      if (bp === undefined) break;
      if (bound !== undefined) {
        const crosses = side === Side.Bid ? bp <= bound : bp >= bound;
        if (!crosses) break;
      }
      const lvl = opp.levelMut(bp);
      if (lvl === undefined) break;
      let emptied = false;
      for (;;) {
        const mi = lvl.head;
        if (mi === NIL) break;
        const mid = pool.id[mi];
        const mqty = pool.qty[mi];
        const q = Math.min(qty, mqty);
        this.emit(sink, { kind: "trade", maker: mid, taker, price: bp, qty: q });
        lvl.total -= q;
        pool.qty[mi] = mqty - q;
        qty -= q;
        if (mqty === q) {
          pool.levelUnlink(lvl, mi);
          map.remove(mid);
          pool.free(mi);
          this.emit(sink, { kind: "closed", orderId: mid, reason: CloseReason.Filled });
        }
        if (qty === 0) break;
      }
      emptied = lvl.head === NIL;
      if (emptied) opp.unlinkLevel(bp);
      if (qty === 0) break;
    }
    return qty;
  }

  /// Insert a resting order (pool slot guaranteed available by the book-full
  /// check at ingest).
  private rest(orderId: OrderId, side: Side, price: Price, qty: Qty, tif: Tif): void {
    const idx = this.pool.alloc();
    this.pool.set(idx, orderId, side, OType.Limit, tif, price, qty);
    const own = side === Side.Bid ? this.bids : this.asks;
    const lvl = own.levelInsert(price);
    lvl.total += qty;
    this.pool.levelPush(lvl, idx);
    this.map.insert(orderId, idx);
  }

  // ---- queries ------------------------------------------------------------

  order(id: OrderId): OrderInfo | undefined {
    const i = this.map.get(id);
    if (i < 0) return undefined;
    return {
      id: this.pool.id[i],
      side: this.pool.side[i],
      price: this.pool.price[i],
      qty: this.pool.qty[i],
    };
  }

  bestBid(): Price | undefined { return this.bids.bestPrice(); }
  bestAsk(): Price | undefined { return this.asks.bestPrice(); }
  orderCount(): number { return this.pool.live; }
  seqNo(): number { return this.seq; }
  levelCount(side: Side): number {
    return (side === Side.Bid ? this.bids : this.asks).len();
  }
  depth(side: Side, n: number) {
    return (side === Side.Bid ? this.bids : this.asks).depth(n);
  }

  // ---- snapshot surface (spec/JOURNAL.md) -----------------------------------

  /// All live orders in book order: bids best→worst then asks best→worst,
  /// FIFO within each level.
  restingOrders(): RestingOrder[] {
    const out: RestingOrder[] = [];
    for (const side of [Side.Bid, Side.Ask]) {
      const idx = side === Side.Bid ? this.bids : this.asks;
      for (const d of idx.depth(Number.MAX_SAFE_INTEGER)) {
        const lvl = idx.levelMut(d.price);
        if (lvl === undefined) continue;
        for (let i = lvl.head; i !== NIL; i = this.pool.next[i]) {
          out.push({
            orderId: this.pool.id[i],
            side: this.pool.side[i],
            price: this.pool.price[i],
            qty: this.pool.qty[i],
            tif: this.pool.tif[i],
          });
        }
      }
    }
    return out;
  }

  /// Rebuild a book from a snapshot: same config, explicit seq, resting
  /// orders replayed in snapshot order (bids then asks, FIFO per level).
  static restore(cfg: BookConfig, seq: number, orders: RestingOrder[]): OrderBook {
    const b = new OrderBook(cfg);
    b.seq = seq;
    for (const o of orders) {
      const idx = b.pool.alloc();
      if (idx === NIL) break;
      b.pool.set(idx, o.orderId, o.side, OType.Limit, o.tif, o.price, o.qty);
      const lvl = (o.side === Side.Bid ? b.bids : b.asks).levelInsert(o.price);
      lvl.total += o.qty;
      b.pool.levelPush(lvl, idx);
      b.map.insert(o.orderId, idx);
    }
    return b;
  }

  config(): BookConfig {
    return this.cfg;
  }

  // ---- internals ------------------------------------------------------------

  private priceOk(price: Price): boolean {
    return this.cfg.index === IndexKind.Ladder
      ? price >= this.cfg.priceMin && price <= this.cfg.priceMax
      : price > 0;
  }

  private wouldCross(side: Side, price: Price): boolean {
    if (side === Side.Bid) {
      const bp = this.asks.bestPrice();
      return bp !== undefined && price >= bp;
    }
    const bp = this.bids.bestPrice();
    return bp !== undefined && price <= bp;
  }

  /// Quantity available on the opposite side within `price` (FOK pre-check).
  private fillable(side: Side, price: Price): Qty {
    const [lo, hi] =
      this.cfg.index === IndexKind.Ladder
        ? [this.cfg.priceMin, this.cfg.priceMax]
        : [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER];
    return side === Side.Bid
      ? this.asks.sumRange(lo, price)
      : this.bids.sumRange(price, hi);
  }

  private emit(sink: Sink, ev: Event): void {
    this.seq++;
    sink.onEvent(this.seq, ev);
  }

  private reject(sink: Sink, orderId: OrderId, reason: RejectReason): void {
    this.emit(sink, { kind: "rejected", orderId, reason });
  }
}
