# orderer-ts

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The TypeScript implementation of [orderer](https://github.com/abhijitkrm/orderer):
an LMAX-Disruptor-style, multi-core order-matching engine around the
[matcher](https://github.com/abhijitkrm/matcher) order book, on Node.js
`worker_threads` and `SharedArrayBuffer`. It needs Node 18+ and no runtime
dependencies, and implements `orderer-spec/1.1`. It is a port of
[orderer-rust](https://github.com/abhijitkrm/orderer-rust), and
**byte-identical** to it: listings, per-partition journals (JSONL and
binary), snapshots and exit codes.

```
publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress plugs
(owner     (SAB ring) (worker)│              (worker:       (owner thread,
 thread)                      └─▶ …           journal +      pumped)
                                              apply)
```

Every stage after ingress runs on its own worker: the router, one engine
per partition, and one I/O worker per journal file. The thread that builds
the pipeline owns it. It publishes, and it runs the egress plugs by
pumping the outboxes whenever it waits on a full ring, drains, snapshots or
shuts down, and on an unref'd timer otherwise. The whole API is
synchronous.

## Quick start

```ts
import { Pipeline, collect, journalConfig, matcher } from "@abhijitkrm/orderer";

const [events, listing] = collect(true);
const p = Pipeline.builder()
  .partitions(2)
  .journal(journalConfig(dir, "binary")) // durable: fsync every 1024 records
  .egress(events) // or acks(...), metrics(...), callback(...), your own Egress
  .build();
p.publish(7, matcher.newLimit(1, matcher.Side.Ask, 100, 10, matcher.Tif.Gtc));
p.publish(7, matcher.newLimit(2, matcher.Side.Bid, 100, 4, matcher.Tif.Gtc));
p.drain(); // applied and delivered
p.snapshot().write(path.join(dir, "books.snap")); // consistent cut: matcher-snap/1 + .meta
p.shutdown();
```

The runnable version is `examples/quickstart.ts`.

## Plug points

| Seam | Type | Built-ins |
|---|---|---|
| Matching core | `MatchingCore` + `CoreFactory`, named by `Builder.core(spec)` | `"fifo"` (matcher-ts `OrderBook` per symbol), `"noop"`, or `"module#export"` loaded in each engine worker |
| Egress | `Egress` + `EgressFactory` (one per partition, owner thread) | `collect`, `callback`, `acks` (durability-gated), `metrics` |
| Routing | `PartitionMap` | hash (spec/ROUTING.md) + table overrides |
| Journals | `JournalConfig`, `FsyncPolicy` | JSONL or binary, group-commit fsync on I/O workers |
| Waiting | `Waits` / `WaitStrategy` | busySpin, yield, backoff, blocking (`Atomics.wait`) |
| Recovery | `recover`, `readSnapshot`, `restore` | snapshot + journals → cores at any P; `Recovery.initial()` resumes a pipeline |

## Limits

- One publishing thread: the pipeline's owner. The ring library supports
  multi-producer rings across workers (`MultiProducer`, tested with four
  worker producers), but a `Pipeline` publishes only from its owner.
- matcher-ts holds ids, prices and quantities as JS numbers. Values beyond
  ±2^53 are rejected as malformed input rather than rounded.

## Build, test, harness

```bash
npm install && npm run build
scripts/test.sh                     # ring, golden, pipeline suites + vectors through the harness tools
scripts/build-harness.sh            # → harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}
scripts/vendored.sh                 # vendored spec/ + vectors/ untouched
```

Cross-implementation proofs (diffuzz, exhaustive, e2e, snapdiff) run from
the spec repo with this repo checked out next to it.

## Performance

`orderbench` follows spec/BENCH.md. Core mode is matcher-ts alone (the
isolated number); pipe mode is the whole engine (the integrated number).
The owner thread decodes every event and runs the plugs, so it, not the
engines, bounds the integrated rate. The spec repo's `docs/RESULTS.md` has
the rows and the cross-language comparison.

See [`docs/DESIGN.md`](docs/DESIGN.md) for the TypeScript-specific design.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
