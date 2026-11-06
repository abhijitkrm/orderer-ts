//! orderernode — one replica of a replicated cluster, as a process. Same
//! command line and client protocol as orderer-rust's orderer-node, orderer-cpp's
//! orderer_node and orderer-go's orderernode, so replicas of all four
//! languages can share a cluster.
//!
//!   orderernode --id N --members 0=HOST:PORT,1=HOST:PORT,... --client HOST:PORT
//!               --dir DIR [--compact-every N] [--digest-every N]
//!               [--fsync on|off] [--audit] [--sm pipeline|recorder] [--partitions P]
//!
//! The state machine is the matching pipeline (default) or the language-neutral
//! RecorderSm, wrapped in DedupSm. --audit appends (log index, first order id)
//! for every applied batch to DIR/applied.log (truncated at start), as the
//! other nodes do. ORDERER_KEY in the environment sets the pre-shared cluster
//! key. Limit: order ids, prices and quantities are exact to 2^53 (matcher-ts's
//! range); the audit log reads the first order id from the wire bytes, so it is
//! exact regardless.
import * as fs from "fs";
import * as path from "path";
import { ClientServer, DedupSm, PipelineSm, RecorderSm, Replica, StateMachine, defaultConfig, parseMembers } from "../src/cluster";
import { Entry } from "../src/consensus";

function die(msg: string): never {
  process.stderr.write(`orderernode: ${msg}\n`);
  process.exit(2);
}

/// Records (index, first order id) of every applied batch, then delegates.
class Audited implements StateMachine {
  constructor(private readonly inner: StateMachine, private readonly fd?: number) {}
  apply(index: number, e: Entry): void {
    if (this.fd !== undefined && e.data.length >= 5 && (e.data[0] === 1 || e.data[0] === 2)) {
      const b = Buffer.from(e.data.buffer, e.data.byteOffset, e.data.length);
      const head = e.data[0] === 1 ? 5 : 21;
      if (b.length >= head + 32 && b.readUInt32LE(head - 4) > 0) {
        const rec = Buffer.alloc(16);
        rec.writeBigUInt64LE(BigInt(index), 0);
        b.copy(rec, 8, head + 8, head + 16); // the first command's order id, exactly as sent
        fs.writeSync(this.fd, rec); // straight to the OS: survives kill -9 of this process
      }
    }
    this.inner.apply(index, e);
  }
  snapshot(i: number) { return this.inner.snapshot(i); }
  restore(i: number, d: Uint8Array) { return this.inner.restore(i, d); }
  digest(i: number) { return this.inner.digest(i); }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  let id: number | undefined, members = "", client = "", dir = "", compact = 0, digest = 0, fsync = true, audit = false, smKind = "pipeline", partitions = 2;
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    const v = () => args[++i] ?? die(`${a} needs a value`);
    const n = () => { const x = Number(v()); return Number.isInteger(x) && x >= 0 ? x : die(`${a} wants a number`); };
    switch (a) {
      case "--id": id = n(); break;
      case "--members": members = v(); break;
      case "--client": client = v(); break;
      case "--dir": dir = v(); break;
      case "--compact-every": compact = n(); break;
      case "--digest-every": digest = n(); break;
      case "--fsync": fsync = v() === "on"; break;
      case "--audit": audit = true; break;
      case "--sm": smKind = v(); break;
      case "--partitions": partitions = n(); break;
      default: die(`unknown argument ${a}`);
    }
  }
  if (id === undefined || !members || !client || !dir) die("--id, --members, --client and --dir are required");
  if (smKind !== "pipeline" && smKind !== "recorder") die(`--sm ${smKind}: want pipeline or recorder`);
  const cfg = defaultConfig(id, parseMembers(members), path.join(dir, "state"));
  cfg.compactEvery = compact;
  cfg.digestEvery = digest;
  cfg.fsync = fsync;
  cfg.maxPending = 1 << 16;
  fs.mkdirSync(dir, { recursive: true });
  const key = process.env.ORDERER_KEY ? Buffer.from(process.env.ORDERER_KEY) : undefined;
  cfg.authKey = key;

  const fd = audit ? fs.openSync(path.join(dir, "applied.log"), "w") : undefined;
  const inner: StateMachine = smKind === "recorder" ? new RecorderSm() : new PipelineSm(partitions);
  let replica: Replica;
  try {
    replica = await Replica.start(cfg, new DedupSm(new Audited(inner, fd)));
  } catch (e) {
    die(`start: ${(e as Error).message}`);
  }
  const server = await ClientServer.listen(client, replica, key).catch((e) => die(`client port: ${(e as Error).message}`));
  process.stdout.write(`orderernode ${id} ready (client port ${client})\n`);
  const bye = () => { void server.shutdown().then(() => replica.stop()).then(() => process.exit(0)); };
  process.on("SIGTERM", bye);
  process.on("SIGINT", bye);
}

void main();
