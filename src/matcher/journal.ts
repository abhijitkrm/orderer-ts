//! Journaling (spec/JOURNAL.md): append-only canonical command/event lines.
//! The journal is I/O policy on top of the deterministic core — it never
//! changes matching semantics.

import { Command, Event, Symbol, OType, eventCanonical, otypeStr, sideStr, tifStr } from "./types";
import { Sink } from "./sink";

/// Canonical command line (SCHEMA.md). `sym` adds `"symbol":N` for the
/// `engine:true` tagged form.
export function commandCanonical(cmd: Command, sym?: Symbol): string {
  const sf = sym === undefined ? "" : `,"symbol":${sym}`;
  switch (cmd.kind) {
    case "new": {
      const px =
        cmd.otype === OType.Limit ? `,"price":${cmd.price},"qty":${cmd.qty}` : `,"qty":${cmd.qty}`;
      return `{"cmd":"new"${sf},"order_id":${cmd.orderId},"side":"${sideStr(cmd.side)}","otype":"${otypeStr(cmd.otype)}"${px},"tif":"${tifStr(cmd.tif)}"}`;
    }
    case "cancel":
      return `{"cmd":"cancel"${sf},"order_id":${cmd.orderId}}`;
    case "replace":
      return `{"cmd":"replace"${sf},"order_id":${cmd.orderId},"price":${cmd.price},"qty":${cmd.qty}}`;
  }
}

/// Append-only command journal: every command is serialized before apply.
export class CmdJournal {
  constructor(
    private readonly write: (line: string) => void,
    private readonly sym?: Symbol,
  ) {}

  record(cmd: Command): void {
    this.write(commandCanonical(cmd, this.sym) + "\n");
  }
}

/// Sink decorator: journals each event line then forwards to the inner sink.
export class EvtJournal implements Sink {
  constructor(
    private readonly inner: Sink,
    private readonly write: (line: string) => void,
  ) {}

  onEvent(seq: number, ev: Event): void {
    this.write(eventCanonical(seq, ev) + "\n");
    this.inner.onEvent(seq, ev);
  }
}

/// Journals one symbol-tagged engine event — for applyTagged callbacks.
export function journalEvent(
  sym: Symbol,
  seq: number,
  ev: Event,
  write: (line: string) => void,
): void {
  write(eventCanonical(seq, ev, sym) + "\n");
}
