//! Preallocated order pool — indices into flat arrays, never per-order
//! objects. Intrusive level links (prev/next) live here too.

import { OrderId, OType, Price, Qty, Side, Tif } from "./types";

export const NIL = -1;

export class Pool {
  readonly cap: number;
  live = 0;

  readonly id: Float64Array;
  readonly side: Int32Array;
  readonly otype: Int32Array;
  readonly tif: Int32Array;
  readonly price: Float64Array;
  readonly qty: Float64Array;
  readonly prev: Int32Array;
  readonly next: Int32Array;

  private readonly freeStack: Int32Array;
  private freeTop: number;

  constructor(cap: number) {
    this.cap = cap;
    this.id = new Float64Array(cap);
    this.side = new Int32Array(cap);
    this.otype = new Int32Array(cap);
    this.tif = new Int32Array(cap);
    this.price = new Float64Array(cap);
    this.qty = new Float64Array(cap);
    this.prev = new Int32Array(cap);
    this.next = new Int32Array(cap);
    this.freeStack = new Int32Array(cap);
    for (let i = 0; i < cap; i++) this.freeStack[i] = cap - 1 - i;
    this.freeTop = cap;
  }

  alloc(): number {
    if (this.freeTop === 0) return NIL;
    this.live++;
    return this.freeStack[--this.freeTop];
  }

  free(i: number): void {
    this.freeStack[this.freeTop++] = i;
    this.live--;
  }

  // ---- intrusive level link ops -------------------------------------------

  levelPush(level: { head: number; tail: number }, i: number): void {
    this.prev[i] = level.tail;
    this.next[i] = NIL;
    if (level.tail !== NIL) this.next[level.tail] = i;
    else level.head = i;
    level.tail = i;
  }

  /// Unlink `i` from anywhere in `level` and adjust the level total.
  levelUnlink(level: { head: number; tail: number; total: number }, i: number): void {
    const p = this.prev[i];
    const n = this.next[i];
    if (p !== NIL) this.next[p] = n;
    else level.head = n;
    if (n !== NIL) this.prev[n] = p;
    else level.tail = p;
    this.prev[i] = this.next[i] = NIL;
    level.total -= this.qty[i];
  }

  set(i: number, id: OrderId, side: Side, otype: OType, tif: Tif, price: Price, qty: Qty): void {
    this.id[i] = id;
    this.side[i] = side;
    this.otype[i] = otype;
    this.tif[i] = tif;
    this.price[i] = price;
    this.qty[i] = qty;
    this.prev[i] = this.next[i] = NIL;
  }
}
