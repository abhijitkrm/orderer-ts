# Changelog

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
