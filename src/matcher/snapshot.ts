//! Snapshot serialization per spec/JOURNAL.md — flat {"rec":...} lines:
//!   {"format":"matcher-snap/1","pmin":P,"pmax":M,"max_orders":N,"index":"..."}
//!   {"rec":"book","symbol":S,"seq":N}
//!   {"rec":"order","order_id":I,"side":"...","otype":"limit","tif":"...",
//!    "price":P,"qty":Q}

import { BookConfig, OrderBook, RestingOrder, defaultConfig } from "./book";
import { Engine } from "./engine";
import { IndexKind } from "./priceindex";
import { Side, Symbol, Tif, sideFromStr, sideStr, tifStr } from "./types";

export function writeBook(b: OrderBook, sym: Symbol, out: string[]): void {
  out.push(`{"rec":"book","symbol":${sym},"seq":${b.seqNo()}}`);
  for (const o of b.restingOrders()) {
    out.push(
      `{"rec":"order","order_id":${o.orderId},"side":"${sideStr(o.side)}","otype":"limit","tif":"${tifStr(o.tif)}","price":${o.price},"qty":${o.qty}}`,
    );
  }
}

export function writeEngine(e: Engine): string {
  const c = e.config();
  const idx = c.index === IndexKind.Tree ? "tree" : "ladder";
  const out = [
    `{"format":"matcher-snap/1","pmin":${c.priceMin},"pmax":${c.priceMax},"max_orders":${c.maxOrders},"index":"${idx}"}`,
  ];
  for (const s of e.symbolList()) writeBook(e.peek(s)!, s, out);
  return out.join("\n") + "\n";
}

export interface SnapBook {
  symbol: Symbol;
  seq: number;
  orders: RestingOrder[];
}

export interface Snap {
  cfg: BookConfig;
  books: SnapBook[];
}

/// Minimal flat-JSON getters — the snapshot format is canonical (fixed field
/// order, no nesting), so substring scan suffices and stays dependency-free.
function getStr(line: string, key: string): string | undefined {
  const m = line.match(new RegExp(`"${key}":"([^"]*)"`));
  return m?.[1];
}
function getNum(line: string, key: string): number | undefined {
  const m = line.match(new RegExp(`"${key}":(-?[0-9]+)`));
  return m === null ? undefined : Number(m[1]);
}

function tifFromStr(s: string | undefined): Tif {
  switch (s) {
    case "ioc": return Tif.Ioc;
    case "fok": return Tif.Fok;
    case "post_only": return Tif.PostOnly;
    default: return Tif.Gtc;
  }
}

export function parseSnapshot(text: string): Snap {
  const snap: Snap = { cfg: defaultConfig(), books: [] };
  let cur: SnapBook | undefined;
  for (const line of text.split("\n")) {
    if (line === "") continue;
    if (line.includes('"rec":"order"')) {
      if (cur === undefined) throw new Error("order record before book record");
      cur.orders.push({
        orderId: getNum(line, "order_id")!,
        side: sideFromStr(getStr(line, "side")) ?? Side.Bid,
        price: getNum(line, "price")!,
        qty: getNum(line, "qty")!,
        tif: tifFromStr(getStr(line, "tif")),
      });
    } else if (line.includes('"rec":"book"')) {
      cur = { symbol: getNum(line, "symbol")!, seq: getNum(line, "seq")!, orders: [] };
      snap.books.push(cur);
    } else {
      snap.cfg = {
        priceMin: getNum(line, "pmin") ?? 0,
        priceMax: getNum(line, "pmax") ?? 1_000_000,
        maxOrders: getNum(line, "max_orders") ?? 65_536,
        index: getStr(line, "index") === "tree" ? IndexKind.Tree : IndexKind.Ladder,
      };
    }
  }
  return snap;
}

/// Rebuild an engine from a parsed snapshot.
export function restoreEngine(s: Snap): Engine {
  const e = new Engine(s.cfg);
  for (const b of s.books) {
    e.addBook(b.symbol, OrderBook.restore(s.cfg, b.seq, b.orders));
  }
  return e;
}
