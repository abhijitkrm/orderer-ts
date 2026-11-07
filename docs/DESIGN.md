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

## Publishing from other threads

The ingress ring is multi-producer (`Atomics.add` claims, per-slot
availability laps). A `Handle` is built from a `HandleDescriptor` (the ring,
a shared control block, the pipeline's absolute-time epoch, and a slot) and
works on any thread; the owner publishes through one too. Each handle's
in-flight flag lives in the control block: shutdown sets the closed flag,
waits until no flag is raised (pumping egress meanwhile, since a blocked
publisher may be waiting for room), then publishes the shutdown control.
A publish that returned Ok is therefore applied, whichever thread made it.

## The owner thread

Egress plugs are user objects, so they run on the owner thread. Each
partition hands its plugs one reused `EvtMsg` view over the outbox slot:
scalar fields are read eagerly, `kind` without decoding, and `ev` is built
on first access. Plugs that keep events copy them (`copyEvtMsg`), as Rust
plugs copy out of a borrowed slot. It pumps
every outbox whenever it is about to wait: in `publish` when ingress is
full, in `drain`, `snapshot` and `shutdown`, and on an unref'd 1 ms timer
when the event loop is idle (so `acks` keep flowing). Decoding events into
objects for the plugs is the integrated pipeline's bottleneck; engines
spend most of their time waiting on the owner.

## The replica runtime (`src/cluster/`)

The threaded runtimes give a replica a driver, a WAL thread and an applier
thread. TypeScript has one event loop per worker, so a replica is one
event-driven object on one thread, with three adaptations:

- **WAL.** Persist effects queue; a batch is written with a synchronous
  `writeSync` and fsynced with the asynchronous `fs.fsync`, which runs on
  libuv's pool. Everything queued while one fsync is in flight goes in the
  next: group commit, as the WAL thread gives the others. Entries are acked
  only after the fsync callback calls `node.persisted`. (Compaction rewrites
  the file synchronously, between fsyncs.)
- **Applying.** Committed entries queue for the state machine and run in
  2 ms slices between I/O polls (`setImmediate`), so a backlog cannot starve
  heartbeats. `StateMachine.apply` may return `false` ("call me again with
  this entry"); `PipelineSm` uses it with the non-blocking `tryPublish`, so a
  full ingress ring (engines busy creating a symbol's first book, which takes
  ~125 ms with the default ladder) never blocks the loop. Without it a
  blocking `publishBatch` held the loop for seconds and the cluster lost its
  leader. What the slicing cannot do is interrupt one `apply` call that itself
  runs longer than the election timeout; and a snapshot or digest
  (`drain` + `snapshot`) runs to completion on the loop.
- **One replica per worker thread** when throughput matters. Several replicas
  in one process (the tests) share a loop; `clusterbench` gives each replica
  its own worker thread, and the leader's worker also runs the producers.

Framing is incremental (`FrameReader` accumulates socket chunks, verifies the
checksum and MAC, and hands whole frames up); sends are dropped when a socket's
write buffer passes 32 MB, as the other runtimes drop on a full queue; a
snapshot streams in 1 MiB chunks with drain-based back-pressure while other
frames to that peer wait behind it. CRC-32C is slicing-by-8 in JS: Node's
`zlib.crc32` is the other polynomial.

## Divergence detection: the event chain

`PipelineSm.digest` is not a hash of the books: it is SHA-256 over the commands applied and one
rolling hash per partition over the events that partition emitted (`spec/REPLICATION.md` 13.16),
kept by an egress plug on the pipeline owner's thread. Reading it needs one `drain()` and no
snapshot (6.3 ms against 229 ms for 300k resting orders). The chain is a u64 held as two u32 halves
because BigInt would allocate on every one of the ~12 steps per event; per event the plug also
decodes the event (`m.ev`). It cannot see a resting order's price until it trades, so
`deepDigest` (the old full-state hash) backs `orderload settle` and the client protocol's hash.
