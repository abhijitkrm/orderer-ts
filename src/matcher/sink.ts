//! The only I/O seam: every book event flows through `onEvent`. Journals,
//! market-data fan-out and gateways attach here — the core never does I/O.

import { Event, eventCanonical, eventFold } from "./types";

export interface Sink {
  onEvent(seq: number, ev: Event): void;
}

/// Records (seq, event) pairs for tests and replay.
export class VecSink implements Sink {
  readonly seqs: number[] = [];
  readonly events: Event[] = [];
  onEvent(seq: number, ev: Event): void {
    this.seqs.push(seq);
    this.events.push(ev);
  }
  /// Canonical stream (one JSON line per event) — the golden-comparable form.
  canonical(): string {
    return this.seqs.map((s, i) => eventCanonical(s, this.events[i])).join("\n") + "\n";
  }
}

/// Folds each event into a checksum — keeps the stream "consumed" during
/// benchmarks without storing it.
export class NullSink implements Sink {
  acc = 0;
  onEvent(seq: number, ev: Event): void {
    this.acc = (this.acc + seq * 0x9e3779b1 + eventFold(ev)) | 0;
  }
}

/// Emits canonical lines to a callback (e.g. process.stdout.write).
export class LinesSink implements Sink {
  constructor(private readonly write: (line: string) => void) {}
  onEvent(seq: number, ev: Event): void {
    this.write(eventCanonical(seq, ev));
  }
}
