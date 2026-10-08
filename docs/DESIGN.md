# orderer-ts design

The architecture is orderer-rust's (its `docs/DESIGN.md`); this file covers
what changes when the threads are Node workers that share no objects.

| orderer-rust | orderer-ts |
|---|---|
| orderer-disruptor | `src/ring.ts` (SharedArrayBuffer rings) |
| orderer-core (core.rs, snapshot) | `src/core.ts`, `src/flat.ts` |
| routing.rs | `src/routing.ts` |
| journal.rs + writer.rs | `src/journal.ts` (`ChunkWriter`, `ioLoop`) |
| msg.rs | `src/msg.ts` (fixed slot layouts) |
| egress.rs | `src/egress.ts` |
| pipeline.rs | `src/pipeline.ts` (owner thread) + `src/worker.ts` (router, engine, I/O roles) |
| recover.rs | `src/recover.ts` |
| harness.rs + bins | `src/harness.ts` + `tools/*.ts` |

## Rings

A ring is one `SharedArrayBuffer`: padded 64-bit sequences (`BigInt64Array`
atomics, loaded once per batch), per-slot availability laps for
multi-producer rings (`Int32Array`), then fixed-size slots read through a
`DataView`. A worker attaches from the ring's descriptor (structured-cloned
via `workerData`). `Atomics` are sequentially consistent, which covers the
protocol's acquire/release. Blocking waits use `Atomics.wait`/`notify`,
which Node allows on the main thread too.

Commands and events cross threads as fixed slot layouts (`src/msg.ts`):
numbers are float64, exact to 2^53, which is matcher-ts's own range.

## Threads

- The router copies ingress slots into the right inbox and stamps iseq.
- Each engine worker builds its own books. Cores cannot cross threads, so
  `Initial` is a recipe: a snapshot body and/or a journal directory, and
  each engine restores and replays just its own partition's symbols.
  `Recovery.initial()` produces it.
- Snapshot replies come back over a `MessagePort` per engine, which the
  owner reads synchronously with `receiveMessageOnPort`.
- Every journal file has an I/O worker. The owner of a `ChunkWriter`
  (engine for commands, the owner thread for events) encodes records into
  64 shared 256 KB chunks and hands them over through two index queues. The
  I/O worker writes everything queued, then decides on one `fs.fsyncSync`
  (libuv uses `F_FULLFSYNC` on macOS), then publishes the `flushed` and
  `durable` watermarks.
- A failing worker records the error in a shared failure block and raises
  the pipeline-wide alert word every ring checks. The owner reports it from
  `drain`, `snapshot` or `shutdown` as `PipelineError("failed")`.

## The owner thread

Egress plugs are user objects, so they run on the owner thread. It pumps
every outbox whenever it is about to wait: in `publish` when ingress is
full, in `drain`, `snapshot` and `shutdown`, and on an unref'd 1 ms timer
when the event loop is idle (so `acks` keep flowing). Decoding events into
objects for the plugs is the integrated pipeline's bottleneck; engines
spend most of their time waiting on the owner.
