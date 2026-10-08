//! Thin multi-symbol router: symbol → OrderBook. Sequencing stays per-book
//! (same contract as the single-symbol core).

import { BookConfig, OrderBook } from "./book";
import { Command, Event, Symbol } from "./types";
import { Sink } from "./sink";

export class Engine {
  private readonly books = new Map<Symbol, OrderBook>();

  constructor(private readonly cfg: BookConfig) {}

  /// Book for `symbol`, created on first use.
  book(symbol: Symbol): OrderBook {
    let b = this.books.get(symbol);
    if (b === undefined) {
      b = new OrderBook(this.cfg);
      this.books.set(symbol, b);
    }
    return b;
  }

  apply(symbol: Symbol, cmd: Command, sink: Sink): void {
    this.book(symbol).apply(cmd, sink);
  }

  /// submit with symbol-tagged delivery: f(symbol, seq, event).
  applyTagged(symbol: Symbol, cmd: Command, f: (sym: Symbol, seq: number, ev: Event) => void): void {
    this.book(symbol).apply(cmd, { onEvent: (seq, ev) => f(symbol, seq, ev) });
  }

  symbols(): number {
    return this.books.size;
  }

  /// Live symbols in ascending order (deterministic for snapshots).
  symbolList(): Symbol[] {
    return [...this.books.keys()].sort((a, b) => a - b);
  }

  /// Book for `symbol` or undefined (snapshot iteration without creation).
  peek(symbol: Symbol): OrderBook | undefined {
    return this.books.get(symbol);
  }

  /// Insert a fully-formed book (snapshot restore).
  addBook(symbol: Symbol, b: OrderBook): void {
    this.books.set(symbol, b);
  }

  config(): BookConfig {
    return this.cfg;
  }
}
