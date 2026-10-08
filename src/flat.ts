//! Strict flat-JSON field access and canonical command lines.
//!
//! matcher-ts's parsing is lenient; orderer's harnesses must reject
//! malformed input exactly as orderer-rust does (spec/HARNESS.md §5), so
//! these mirror matcher-rust's jsonflat: a missing or unparsable field is an
//! error (undefined). matcher-ts holds ids, prices and quantities as JS
//! numbers, so integers beyond ±2^53 are rejected rather than rounded.

import { BookConfig, defaultConfig } from "./matcher/book";
import { IndexKind } from "./matcher/priceindex";
import { Command, OType, Side, Tif, otypeStr, sideStr, tifStr } from "./matcher/types";

/// Value of `key`: the quoted string's contents, or the trimmed token.
export function get(line: string, key: string): string | undefined {
  const pat = `"${key}":`;
  const p = line.indexOf(pat);
  if (p < 0) return undefined;
  const start = p + pat.length;
  if (line.charCodeAt(start) === 34) {
    const end = line.indexOf('"', start + 1);
    return end < 0 ? undefined : line.slice(start + 1, end);
  }
  let end = line.length;
  for (let i = start; i < line.length; i++) {
    const c = line.charCodeAt(i);
    if (c === 44 || c === 125) {
      end = i;
      break;
    }
  }
  return line.slice(start, end).trim();
}

const DIGITS = /^[0-9]+$/;

/// Unsigned decimal (optional '+'), at most 2^53 - 1.
export function parseU64(s: string | undefined): number | undefined {
  if (s === undefined) return undefined;
  if (s.startsWith("+")) s = s.slice(1);
  if (!DIGITS.test(s)) return undefined;
  const v = Number(s);
  return Number.isSafeInteger(v) ? v : undefined;
}

export function parseI64(s: string | undefined): number | undefined {
  if (s === undefined || s === "") return undefined;
  const neg = s[0] === "-";
  const body = s[0] === "-" || s[0] === "+" ? s.slice(1) : s;
  if (!DIGITS.test(body)) return undefined;
  const v = Number(body);
  if (!Number.isSafeInteger(v)) return undefined;
  return neg ? -v : v;
}

export const u64 = (line: string, key: string) => parseU64(get(line, key));
export const i64 = (line: string, key: string) => parseI64(get(line, key));

export function parseTif(s: string | undefined): Tif | undefined {
  switch (s) {
    case "gtc": return Tif.Gtc;
    case "ioc": return Tif.Ioc;
    case "fok": return Tif.Fok;
    case "post_only": return Tif.PostOnly;
  }
  return undefined;
}

/// One canonical command line; undefined if any field is missing or invalid.
export function parseCommand(line: string): Command | undefined {
  switch (get(line, "cmd")) {
    case "new": {
      const side = get(line, "side"), otype = get(line, "otype"), tif = parseTif(get(line, "tif"));
      const orderId = u64(line, "order_id"), price = i64(line, "price"), qty = u64(line, "qty");
      if (orderId === undefined || price === undefined || qty === undefined || tif === undefined) return undefined;
      const s = side === "bid" ? Side.Bid : side === "ask" ? Side.Ask : undefined;
      const o = otype === "limit" ? OType.Limit : otype === "market" ? OType.Market : undefined;
      if (s === undefined || o === undefined) return undefined;
      return { kind: "new", orderId, side: s, otype: o, price, qty, tif };
    }
    case "cancel": {
      const orderId = u64(line, "order_id");
      return orderId === undefined ? undefined : { kind: "cancel", orderId };
    }
    case "replace": {
      const orderId = u64(line, "order_id"), price = i64(line, "price"), qty = u64(line, "qty");
      if (orderId === undefined || price === undefined || qty === undefined) return undefined;
      return { kind: "replace", orderId, price, qty };
    }
  }
  return undefined;
}

/// Corpus/vector header → book config (matcher defaults).
export function parseHeader(line: string): BookConfig {
  const d = defaultConfig();
  return {
    priceMin: i64(line, "pmin") ?? d.priceMin,
    priceMax: i64(line, "pmax") ?? d.priceMax,
    maxOrders: u64(line, "max_orders") ?? d.maxOrders,
    index: get(line, "index") === "tree" ? IndexKind.Tree : IndexKind.Ladder,
  };
}

export const indexName = (k: IndexKind) => (k === IndexKind.Tree ? "tree" : "ladder");

export function sameBook(a: BookConfig, b: BookConfig): boolean {
  return a.priceMin === b.priceMin && a.priceMax === b.priceMax && a.maxOrders === b.maxOrders && a.index === b.index;
}

/// matcher's canonical command line; the engine form (with "symbol") when sym is given.
export function writeCommand(c: Command, sym?: number): string {
  const sf = sym === undefined ? "" : `,"symbol":${sym}`;
  switch (c.kind) {
    case "new":
      return `{"cmd":"new"${sf},"order_id":${c.orderId},"side":"${sideStr(c.side)}","otype":"${otypeStr(c.otype)}","price":${c.price},"qty":${c.qty},"tif":"${tifStr(c.tif)}"}`;
    case "cancel":
      return `{"cmd":"cancel"${sf},"order_id":${c.orderId}}`;
    case "replace":
      return `{"cmd":"replace"${sf},"order_id":${c.orderId},"price":${c.price},"qty":${c.qty}}`;
  }
}
