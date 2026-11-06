// The client protocol, wire-compatible with orderer-rust (client.rs),
// orderer-cpp and orderer-go: requests PROPOSE / STATUS / HASH / BARRIER over
// the framing of net.ts, a leader-following Client, and exactly-once sessions.
// A server holding a cluster key requires the challenge-response handshake.

import * as net from "net";
import { NONE, NodeId } from "../consensus";
import { ctEq, handshake, MAC_LEN, nonce, NONCE_LEN, Session } from "./auth";
import { CmdSym, encodeBatch, Out, SessionKey } from "./codec";
import { Frame, FrameReader, NetError, parseAddr, putFrame } from "./net";
import { Replica } from "./replica";
import { Digest } from "./statemachine";
import { Member } from "./transport";

export const Q_HELLO = 0, Q_PROPOSE = 1, Q_STATUS = 2, Q_HASH = 3, Q_BARRIER = 4;
export const CR_OK = 1, CR_NOT_LEADER = 2, CR_BUSY = 3, CR_UNKNOWN = 4, CR_ERR = 5, CR_TEXT = 6, CR_HASH = 7, CR_CHALLENGE = 8;

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const text = (s: string) => Buffer.from(s);

/// Serves one replica's client port. The Replica must outlive the server.
export class ClientServer {
  private readonly server = net.createServer();
  private readonly socks = new Set<net.Socket>();
  private constructor(private readonly replica: Replica, private readonly key?: Uint8Array) {}

  static async listen(addr: string, replica: Replica, key?: Uint8Array): Promise<ClientServer> {
    const s = new ClientServer(replica, key);
    const { host, port } = parseAddr(addr);
    await new Promise<void>((res, rej) => {
      s.server.once("error", rej);
      s.server.listen(port, host, () => { s.server.off("error", rej); res(); });
    });
    s.server.on("connection", (sock) => s.serve(sock));
    return s;
  }

  async shutdown(): Promise<void> {
    for (const s of this.socks) s.destroy();
    await new Promise<void>((res) => this.server.close(() => res()));
  }

  private serve(sock: net.Socket): void {
    sock.setNoDelay(true);
    this.socks.add(sock);
    sock.on("close", () => this.socks.delete(sock));
    sock.on("error", () => sock.destroy());
    const reader = new FrameReader();
    let sess: Session | undefined;
    let stage: "hello" | "ready" = this.key ? "hello" : "ready";
    const hello = this.key ? setTimeout(() => sock.destroy(), 5000) : undefined;
    sock.on("close", () => hello && clearTimeout(hello));
    // Requests are answered strictly in order, one at a time.
    const queue: Frame[] = [];
    let busy = false;
    const reply = (kind: number, body: Uint8Array) => { if (!sock.destroyed) sock.write(putFrame(kind, body, sess)); };

    const answer = (r: Awaited<ReturnType<Replica["propose"]>>) => {
      switch (r.kind) {
        case "ok": reply(CR_OK, new Out(8).u64(r.index).done()); break;
        case "notleader": reply(CR_NOT_LEADER, Buffer.from([r.hint === NONE ? 255 : r.hint])); break;
        case "stopped": reply(CR_NOT_LEADER, Buffer.from([255])); break;
        case "busy": reply(CR_BUSY, Buffer.alloc(0)); break;
        default: reply(CR_UNKNOWN, text("unknown outcome"));
      }
    };

    const handle = async (f: Frame): Promise<void> => {
      switch (f.kind) {
        case Q_HELLO: reply(CR_ERR, text("this server does not use authentication")); sock.end(); break;
        case Q_PROPOSE: answer(await this.replica.propose(f.body)); break;
        case Q_BARRIER: answer(await this.replica.barrier()); break;
        case Q_STATUS: {
          const s = this.replica.status();
          reply(CR_TEXT, text(`id=${s.id} role=${s.role} term=${s.term} leader=${s.leader} commit=${s.commit} applied=${s.applied} ` +
            `base=${s.base} last=${s.lastIndex} pending=${s.pending} halted=${s.halted === "" ? "-" : s.halted} digests_verified=${s.digestsVerified}\n`));
          break;
        }
        case Q_HASH: {
          const h = await this.replica.stateHash();
          if (h) reply(CR_HASH, new Out(40).u64(h.index).bytes(h.digest).done());
          else reply(CR_ERR, text("state hash unsupported"));
          break;
        }
        default: reply(CR_ERR, text("unknown request"));
      }
    };

    const run = async () => {
      if (busy) return;
      busy = true;
      try {
        while (queue.length > 0 && !sock.destroyed) await handle(queue.shift()!);
      } finally {
        busy = false;
      }
    };

    sock.on("data", (chunk: Buffer) => {
      try {
        reader.push(chunk, (f) => {
          if (stage === "hello") {
            if (hello) clearTimeout(hello);
            if (f.kind !== Q_HELLO || f.body.length !== NONCE_LEN) throw new NetError("hello");
            const ns = nonce();
            const mac = handshake(this.key!, "cchal", f.body, ns);
            sock.write(putFrame(CR_CHALLENGE, Buffer.concat([ns, mac.subarray(0, MAC_LEN)])));
            sess = new Session(handshake(this.key!, "csess", f.body, ns), 1);
            reader.sess = sess;
            stage = "ready";
            return;
          }
          queue.push(f);
        });
      } catch {
        sock.destroy();
        return;
      }
      void run();
    });
  }
}

export type ClientKind = "ok" | "noleader" | "unknown" | "protocol";
export interface ClientResult { kind: ClientKind; index: number; text: string }
const cres = (kind: ClientKind, index = 0, text = ""): ClientResult => ({ kind, index, text });

/// A connection to one replica's client port.
export class Conn {
  private readonly reader = new FrameReader();
  private waiting?: { resolve: (f: Frame | undefined) => void; timer: NodeJS.Timeout };
  private inbox: Frame[] = [];
  private dead = false;

  private constructor(private readonly sock: net.Socket) {
    sock.setNoDelay(true);
    sock.on("data", (chunk: Buffer) => {
      try {
        this.reader.push(chunk, (f) => this.deliver(f));
      } catch {
        this.die();
      }
    });
    sock.on("close", () => this.die());
    sock.on("error", () => sock.destroy());
  }

  private deliver(f: Frame): void {
    const w = this.waiting;
    if (w) { this.waiting = undefined; clearTimeout(w.timer); w.resolve(f); } else this.inbox.push(f);
  }

  private die(): void {
    this.dead = true;
    const w = this.waiting;
    if (w) { this.waiting = undefined; clearTimeout(w.timer); w.resolve(undefined); }
  }

  private next(ms: number): Promise<Frame | undefined> {
    const f = this.inbox.shift();
    if (f) return Promise.resolve(f);
    if (this.dead) return Promise.resolve(undefined);
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this.waiting = undefined; this.sock.destroy(); resolve(undefined); }, ms);
      this.waiting = { resolve, timer };
    });
  }

  /// Open a connection, authenticating when `key` is set; undefined on failure.
  static async connect(addr: string, key?: Uint8Array): Promise<Conn | undefined> {
    const { host, port } = parseAddr(addr);
    const sock = await new Promise<net.Socket | undefined>((resolve) => {
      const s = net.connect({ host, port });
      const t = setTimeout(() => { s.destroy(); resolve(undefined); }, 500);
      s.once("connect", () => { clearTimeout(t); resolve(s); });
      s.once("error", () => { clearTimeout(t); s.destroy(); resolve(undefined); });
    });
    if (!sock) return undefined;
    const c = new Conn(sock);
    if (key) {
      const nc = nonce();
      sock.write(putFrame(Q_HELLO, nc));
      const f = await c.next(30000);
      if (!f || f.kind !== CR_CHALLENGE || f.body.length !== NONCE_LEN + MAC_LEN) { c.close(); return undefined; }
      const want = handshake(key, "cchal", nc, f.body.subarray(0, NONCE_LEN));
      if (!ctEq(want.subarray(0, MAC_LEN), f.body.subarray(NONCE_LEN))) { c.close(); return undefined; }
      c.sess = new Session(handshake(key, "csess", nc, f.body.subarray(0, NONCE_LEN)), 0);
      c.reader.sess = c.sess;
    }
    return c;
  }

  private sess?: Session;

  close(): void { this.sock.destroy(); }

  /// undefined on any I/O or protocol failure.
  async call(kind: number, body: Uint8Array = new Uint8Array(0)): Promise<Frame | undefined> {
    if (this.dead) return undefined;
    this.sock.write(putFrame(kind, body, this.sess));
    return this.next(30000);
  }

  async status(): Promise<string | undefined> {
    const f = await this.call(Q_STATUS);
    return f && f.kind === CR_TEXT ? f.body.toString() : undefined;
  }

  /// A linearizable read barrier (this node must be the leader).
  async barrier(): Promise<number | undefined> {
    const f = await this.call(Q_BARRIER);
    return f && f.kind === CR_OK && f.body.length === 8 ? f.body.readUInt32LE(0) + f.body.readUInt32LE(4) * 4294967296 : undefined;
  }

  async hash(): Promise<{ index: number; digest: Digest } | undefined> {
    const f = await this.call(Q_HASH);
    if (!f || f.kind !== CR_HASH || f.body.length !== 40) return undefined;
    return { index: f.body.readUInt32LE(0) + f.body.readUInt32LE(4) * 4294967296, digest: Buffer.from(f.body.subarray(8)) };
  }
}

/// Submits batches to a cluster, following leader hints and surviving restarts.
export class Client {
  private conns: Array<Conn | undefined>;
  private current = 0;
  private key?: Uint8Array;
  private session?: SessionKey;
  private pending?: Buffer;
  /// Bounds proposeSession's retries.
  retryBudgetMs = 30000;

  constructor(private readonly addrs: Member[]) {
    this.conns = new Array(addrs.length).fill(undefined);
  }

  withKey(key: Uint8Array): this { this.key = key; return this; }
  /// Exactly-once submission under session id `id` (one sequential stream per id).
  withSession(id: bigint | number): this { this.session = { client: BigInt(id), seq: 0n }; return this; }

  close(): void {
    this.conns.forEach((c, i) => { c?.close(); this.conns[i] = undefined; });
  }

  /// Submit an encoded payload. ok = committed on a quorum. unknown = may still commit.
  async propose(payload: Uint8Array): Promise<ClientResult> {
    for (let attempt = 0; attempt < 600; attempt++) {
      const i = this.current % this.addrs.length;
      let c = this.conns[i];
      if (!c) c = this.conns[i] = await Conn.connect(this.addrs[i].addr, this.key);
      if (!c) { this.current++; await sleep(10); continue; }
      const f = await c.call(Q_PROPOSE, payload);
      if (!f) {
        // Died mid-request: it may have been accepted. Unknown, never a blind retry.
        c.close();
        this.conns[i] = undefined;
        this.current++;
        return cres("unknown", 0, "connection lost");
      }
      switch (f.kind) {
        case CR_OK:
          if (f.body.length !== 8) return cres("protocol", 0, "bad reply");
          return cres("ok", f.body.readUInt32LE(0) + f.body.readUInt32LE(4) * 4294967296);
        case CR_NOT_LEADER: {
          const pos = f.body.length > 0 ? this.addrs.findIndex((a) => a.id === f.body[0]) : -1;
          if (pos >= 0) this.current = pos; else this.current++;
          await sleep(5);
          break;
        }
        case CR_BUSY: await sleep(2); break;
        case CR_UNKNOWN:
          c.close();
          this.conns[i] = undefined;
          return cres("unknown", 0, f.body.toString());
        default: return cres("protocol", 0, `reply ${f.kind}`);
      }
    }
    return cres("noleader");
  }

  /// Submit a batch with exactly-once effect: the same (session, seq) is
  /// retried through lost connections and leader changes until it is
  /// definitely committed.
  async proposeSession(cmds: ReadonlyArray<CmdSym>): Promise<ClientResult> {
    if (!this.session) return cres("protocol", 0, "no session: call withSession first");
    if (!this.pending) {
      this.session.seq += 1n;
      this.pending = encodeBatch(cmds, { client: this.session.client, seq: this.session.seq });
    }
    const deadline = Date.now() + this.retryBudgetMs;
    for (;;) {
      const r = await this.propose(this.pending);
      if (r.kind === "ok") { this.pending = undefined; return r; }
      if (r.kind === "protocol" || Date.now() >= deadline) return r;
      await sleep(50);
    }
  }
}

/// Parse "ID=HOST:PORT,ID=HOST:PORT,...".
export function parseMembers(s: string): Member[] {
  return s.split(",").map((m) => {
    const eq = m.indexOf("=");
    if (eq < 0) throw new Error(`want ID=HOST:PORT, got ${m}`);
    const id = Number(m.slice(0, eq));
    if (!Number.isInteger(id) || id < 0 || id > 254) throw new Error(`bad node id ${m.slice(0, eq)}`);
    return { id, addr: m.slice(eq + 1).replace("localhost", "127.0.0.1") };
  });
}

export { Frame, Digest };
