# Changelog

## Unreleased: orderer-spec/1.2

- Binary journals are version 2 (CRC-32C per record); version 1 still reads.
- `repairDir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline.checkpoint()` rotates journals onto segments at a clean cut (the
  I/O worker switches files on a ROTATE marker), writes the snapshot durably
  and removes covered segments.
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
