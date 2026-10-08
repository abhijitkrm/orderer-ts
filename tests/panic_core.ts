// A core that throws on a poison order id (loaded by engine workers by path).
import { BookConfig } from "../src/matcher/book";
import { Command } from "../src/matcher/types";
import { Emit, FifoCore, MatchingCore } from "../src/core";

export function panicCore(cfg: BookConfig): MatchingCore {
  const inner = new FifoCore(cfg);
  return {
    apply(sym: number, cmd: Command, emit: Emit): void {
      if (cmd.kind === "cancel" && cmd.orderId === 666) throw new Error("poison command");
      inner.apply(sym, cmd, emit);
    },
    snapshotBlocks: (out) => inner.snapshotBlocks(out),
    restoreBook: (s, q, o) => inner.restoreBook(s, q, o),
  };
}
