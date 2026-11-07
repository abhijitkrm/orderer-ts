# Changelog

## Unreleased

- Event-chain digest (`spec/REPLICATION.md` 13.16): each partition keeps a rolling hash over the events it emits (`eventDigest` egress plug, u64 arithmetic in 32-bit pairs, checked against a BigInt reference), so a replica's `digest` costs one pipeline barrier instead of a snapshot of every book: 6.3 ms against 229 ms (`deepDigest`) with 300k resting orders. `StateMachine.deepDigest` is the full-state hash and backs the client protocol's state hash; `PipelineSm` snapshots are now `PSM2` (they carry the chains) and restore refuses other layouts or partition counts. Conformance vectors `vectors/replication-digest/` match Rust's. 7 new tests (`tests/event_digest.ts`); mixed TS/Rust clusters at `--digest-every 64` survive `kill -9`, cross-language snapshot transfer, authentication.
- `src/consensus.ts`: the replication protocol core (`spec/REPLICATION.md`) as a pure state machine, ported from orderer-rust and proved against the shared conformance vectors (`vectors/replication/`: 28 traces, 28,000 recorded drain points, byte-identical effects and state; the 64-bit generator uses BigInt).
- `src/cluster/`: the replication runtime, ported from orderer-rust, orderer-cpp and orderer-go (`spec/REPLICATION.md` §13.14). CRC-32C (slicing-by-8), WAL and snapshot store (byte-identical to Rust's, against `vectors/replication-wire/`), TCP transport with the pre-shared-key handshake and per-frame MAC, replica (one event-driven object: group-commit WAL with an off-loop fsync, time-sliced applies, `Busy` back-pressure, digest-based divergence halt, compaction and snapshot install), client protocol with exactly-once sessions and linearizable `barrier()`, `RecorderSm`, `DedupSm` and `PipelineSm` (the matching pipeline as the replicated state machine, same state digest as Rust, C++ and Go), and scale-out (`ShardMap`, `ShardedClient`, group assignment checked against independently computed values). `StateMachine.apply` may return `false` to be called again, so a full pipeline ring never blocks the loop. 20 end-to-end tests, 10 wire tests, 6 shard tests; mixed TS/Rust/Go/C++ clusters survive `kill -9` of the leader, authentication and cross-language snapshot transfer (`scripts/interop.sh`).
- `tools/orderernode.ts` (one replica as a process, same command line as the other nodes), `tools/orderload.ts` (the port of `orderer-load`; order ids stay below 2^53) and `tools/clusterbench.ts` (same flags and output as orderer-rust's, one worker thread per replica). On a loaded M1, best of 3-5 interleaved with Rust, 3 nodes: no-op state machine 2.3M commands/s against 8.3M, matching pipeline 0.9M against 8.0M; durable (fsync on a quorum, window 256, 4 producers) no-op 3.7M against 14.0M. JS cost (frame CRC, copies, one thread per replica, the JS matcher), not the protocol.
- `Node.entryAt` is public. Vendored spec and vectors re-pinned to orderer ae0ed4a (adds `vectors/replication-wire/`).

- `orderbench --latency on|off`. `off` stops stamping and sampling commands, which
  the BENCH.md protocol does with two clock reads per command; default `on`.
  Journals-off pipeline, M1: P=2 no-op core 8.9M -> 14.7M, real core P=3
  7.5M -> 10.1M.

## 0.2.1 (orderer-spec/1.3)

- Vendors matcher-ts 223341e: the price ladder finds the next best price through a
  summary bitmap, and an emptied side resets at once. A book whose side
  emptied used to scan the whole ladder per cancel (W3 at 1M ops: 4-24x
  faster in the matcher bench). Output unchanged.
- Repair (orderer-spec 1.3), for three kinds of damage SIGKILL left that
  `--repair` refused: binary tails of all-zero records (an interrupted
  write on macOS can extend a file with zeros, a whole write buffer of
  them) are cut; a segment created at a checkpoint whose header never
  arrived is deleted; and when the last segment holds no records, the
  torn segment before it is repaired too.

## 0.2.0 (orderer-spec/1.2)

- Binary journals are version 2 (CRC-32C per record); version 1 still reads.
- `repairDir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline.checkpoint()` rotates journals onto segments at a clean cut (the
  I/O worker switches files on a ROTATE marker), writes the snapshot durably
  and removes covered segments.
- `Pipeline.stats()` (stats.ts): ring depths, per-partition counts,
  watermarks, fsync timings; `toPrometheus(stats)`.
- `Builder.checkpointEvery(ms)`: automatic checkpoints from the owner's
  event loop while it is idle.
- `orderrun --checkpoint-every K` and `--durable`; `scripts/test.sh` runs the
  vendored `spec/conformance.sh`.
- Publishing from any thread: multi-producer ingress, `Handle` /
  `Pipeline.handleDescriptor()` / `pumpWhile`; `orderbench --producers N`
  uses worker producers.
- Harness corpora are parsed line by line from a Buffer (files over V8's
  string limit).

## 0.1.0

- First release: the full orderer pipeline in TypeScript on worker_threads
  and SharedArrayBuffer, byte-identical to orderer-rust 0.1
  (`orderer-spec/1.1`).
- Vendors matcher-ts `0ef59a2` and the orderer spec `41019c6`.
- Harness tools (spec/HARNESS.md) and `orderbench` (spec/BENCH.md 1.1).
