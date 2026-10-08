//! Price index: direct-indexed bitmap ladder for bounded ranges (O(1) best
//! price via a top-of-book cursor), sorted-keys Map fallback for unbounded
//! prices. Enum-dispatched — no virtuals in the match loop.

import { Price, Qty, Side } from "./types";
import { NIL } from "./pool";

export interface Level {
  head: number;
  tail: number;
  total: number;
}

export const emptyLevel = (): Level => ({ head: NIL, tail: NIL, total: 0 });
const isEmpty = (l: Level) => l.head === NIL;

export interface LevelDepth {
  price: Price;
  qty: Qty;
}

export enum IndexKind {
  Ladder,
  Tree,
}

// ---------------------------------------------------------------------------
// Ladder: levels[p - base] + occupancy bitmap + best-price cursor.
// ---------------------------------------------------------------------------

export class LadderIndex {
  readonly side: Side;
  readonly base: Price;
  readonly levels: Level[];
  readonly bits: Uint32Array;
  best = NIL;
  count = 0;

  constructor(side: Side, pmin: Price, pmax: Price) {
    this.side = side;
    this.base = pmin;
    const span = pmax - pmin + 1;
    this.levels = new Array<Level>(span);
    for (let i = 0; i < span; i++) this.levels[i] = emptyLevel();
    this.bits = new Uint32Array((span + 31) >> 5);
  }

  private idx(p: Price): number {
    return p - this.base;
  }

  bestPrice(): Price | undefined {
    if (this.best === NIL) return undefined;
    return this.base + this.best;
  }

  levelMut(p: Price): Level | undefined {
    const i = this.idx(p);
    if (i < 0 || i >= this.levels.length) return undefined;
    const l = this.levels[i];
    return isEmpty(l) ? undefined : l;
  }

  levelInsert(p: Price): Level {
    const i = this.idx(p);
    const l = this.levels[i];
    if (isEmpty(l)) {
      this.bits[i >> 5] |= 1 << (i & 31);
      this.count++;
      if (this.best === NIL ||
          (this.side === Side.Ask ? i < this.best : i > this.best)) {
        this.best = i;
      }
    }
    return l;
  }

  unlinkLevel(p: Price): void {
    const i = this.idx(p);
    if (i < 0 || i >= this.levels.length || !isEmpty(this.levels[i])) return;
    this.bits[i >> 5] &= ~(1 << (i & 31));
    this.count--;
    if (i === this.best) this.best = this.rescan(i);
  }

  /// Nearest non-empty level strictly beyond `from`; NIL if the book emptied.
  private rescan(from: number): number {
    const n = this.levels.length;
    if (this.side === Side.Ask) {
      for (let w = from >> 5; w < this.bits.length; w++) {
        let word = this.bits[w];
        if (w === from >> 5) {
          const b = (from & 31) + 1;
          word &= b === 32 ? 0 : ~0 << b;
        }
        if (word !== 0) {
          const i = (w << 5) + (31 - Math.clz32(word & -word));
          return i < n ? i : NIL;
        }
      }
    } else {
      for (let w = from >> 5; w >= 0; w--) {
        let word = this.bits[w];
        if (w === from >> 5) {
          const b = from & 31;
          word &= b === 0 ? 0 : ~0 >>> (32 - b);
        }
        if (word !== 0) {
          const i = (w << 5) + (31 - Math.clz32(word));
          return i < n ? i : NIL;
        }
      }
    }
    return NIL;
  }

  sumRange(lo: Price, hi: Price): Qty {
    const loI = Math.max(0, this.idx(lo));
    const hiI = Math.min(this.levels.length - 1, this.idx(hi));
    let sum = 0;
    for (let i = loI; i <= hiI; i++) sum += this.levels[i].total;
    return sum;
  }

  len(): number {
    return this.count;
  }

  depth(n: number): LevelDepth[] {
    const out: LevelDepth[] = [];
    if (this.side === Side.Ask) {
      for (let w = 0; w < this.bits.length && out.length < n; w++) {
        let word = this.bits[w];
        while (word !== 0 && out.length < n) {
          const b = 31 - Math.clz32(word & -word); // lowest set bit
          const i = (w << 5) + b;
          out.push({ price: this.base + i, qty: this.levels[i].total });
          word &= word - 1;
        }
      }
    } else {
      for (let w = this.bits.length - 1; w >= 0 && out.length < n; w--) {
        let word = this.bits[w];
        while (word !== 0 && out.length < n) {
          const b = 31 - Math.clz32(word); // highest set bit
          const i = (w << 5) + b;
          out.push({ price: this.base + i, qty: this.levels[i].total });
          word &= ~(1 << b);
        }
      }
    }
    return out;
  }
}

// ---------------------------------------------------------------------------
// Tree: Map<price, Level> + sorted key array (unbounded price domain).
// ---------------------------------------------------------------------------

export class TreeIndex {
  readonly side: Side;
  readonly map = new Map<Price, Level>();
  private keys: number[] = [];

  constructor(side: Side) {
    this.side = side;
  }

  private findKey(p: Price): number {
    let lo = 0, hi = this.keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.keys[mid] < p) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  bestPrice(): Price | undefined {
    if (this.keys.length === 0) return undefined;
    return this.side === Side.Bid ? this.keys[this.keys.length - 1] : this.keys[0];
  }

  levelMut(p: Price): Level | undefined {
    const l = this.map.get(p);
    return l === undefined || isEmpty(l) ? undefined : l;
  }

  levelInsert(p: Price): Level {
    let l = this.map.get(p);
    if (l === undefined) {
      l = emptyLevel();
      this.map.set(p, l);
      this.keys.splice(this.findKey(p), 0, p);
    }
    return l;
  }

  unlinkLevel(p: Price): void {
    const l = this.map.get(p);
    if (l !== undefined && isEmpty(l)) {
      this.map.delete(p);
      this.keys.splice(this.findKey(p), 1);
    }
  }

  sumRange(lo: Price, hi: Price): Qty {
    let sum = 0;
    for (let i = this.findKey(lo); i < this.keys.length && this.keys[i] <= hi; i++) {
      sum += this.map.get(this.keys[i])!.total;
    }
    return sum;
  }

  len(): number {
    return this.map.size;
  }

  depth(n: number): LevelDepth[] {
    const out: LevelDepth[] = [];
    if (this.side === Side.Ask) {
      for (let i = 0; i < this.keys.length && out.length < n; i++) {
        out.push({ price: this.keys[i], qty: this.map.get(this.keys[i])!.total });
      }
    } else {
      for (let i = this.keys.length - 1; i >= 0 && out.length < n; i--) {
        out.push({ price: this.keys[i], qty: this.map.get(this.keys[i])!.total });
      }
    }
    return out;
  }
}

// ---------------------------------------------------------------------------
// Enum-dispatched index (mirrors Rust `enum PriceIndex` / C++ struct).
// ---------------------------------------------------------------------------

export class PriceIndex {
  readonly kind: IndexKind;
  private readonly lad: LadderIndex;
  private readonly tree: TreeIndex;

  constructor(kind: IndexKind, side: Side, pmin: Price, pmax: Price) {
    this.kind = kind;
    this.lad = new LadderIndex(side, pmin, pmax);
    this.tree = new TreeIndex(side);
  }

  static ladder(side: Side, pmin: Price, pmax: Price): PriceIndex {
    return new PriceIndex(IndexKind.Ladder, side, pmin, pmax);
  }

  static tree(side: Side): PriceIndex {
    return new PriceIndex(IndexKind.Tree, side, 0, 0);
  }

  private get impl(): LadderIndex | TreeIndex {
    return this.kind === IndexKind.Ladder ? this.lad : this.tree;
  }

  bestPrice(): Price | undefined { return this.impl.bestPrice(); }
  levelMut(p: Price): Level | undefined { return this.impl.levelMut(p); }
  levelInsert(p: Price): Level { return this.impl.levelInsert(p); }
  unlinkLevel(p: Price): void { this.impl.unlinkLevel(p); }
  sumRange(lo: Price, hi: Price): Qty { return this.impl.sumRange(lo, hi); }
  len(): number { return this.impl.len(); }
  depth(n: number): LevelDepth[] { return this.impl.depth(n); }
}
