// TCP transport between replicas, wire-compatible with orderer-rust
// (transport.rs), orderer-cpp and orderer-go. Sends never block the
// protocol: a full socket buffer or a down peer drops messages, which the
// protocol tolerates; each peer's connection reconnects with backoff.
// Snapshots stream in chunks and are verified by length and checksum before
// they are accepted. With a cluster key, every connection runs the
// challenge-response handshake of auth.ts and every frame is MAC'd.

import * as net from "net";
import { Msg, NodeId } from "../consensus";
import { ctEq, handshake, MAC_LEN, nonce, NONCE_LEN, Session } from "./auth";
import { CodecError, decodeMsg, encodeMsg, Out, Reader } from "./codec";
import { Frame, FrameReader, NetError, parseAddr, putFrame } from "./net";
import { Incoming, SnapStore } from "./snapstore";

export const K_MSG = 1, K_SNAP_BEGIN = 2, K_SNAP_CHUNK = 3, K_SNAP_END = 4, K_DIGEST = 5, K_HELLO = 6, K_CHALLENGE = 7, K_AUTH = 8;
const SNAP_CHUNK = 1 << 20;
const PEER_QUEUE = 8192;
const MAX_SOCKET_BACKLOG = 32 << 20; // bytes queued in a socket before sends are dropped

export interface Member { id: NodeId; addr: string }

/// What the transport hands to the node.
export type Inbound =
  | { kind: "msg"; from: NodeId; msg: Msg }
  | { kind: "snapshot"; from: NodeId; term: number; lastTerm: number; index: number }
  | { kind: "digest"; from: NodeId; index: number; digest: Buffer };

/// Test hook that cuts links: allows(from, to) is checked on every send and receive.
export class Filter {
  private blocked = new Set<string>();
  block(a: NodeId, b: NodeId): void { this.blocked.add(`${a}>${b}`); this.blocked.add(`${b}>${a}`); }
  isolate(n: NodeId, all: NodeId[]): void { for (const o of all) if (o !== n) this.block(n, o); }
  heal(): void { this.blocked.clear(); }
  allows(from: NodeId, to: NodeId): boolean { return !this.blocked.has(`${from}>${to}`); }
}

export class NetStats {
  sent = 0; dropped = 0; received = 0; badFrames = 0;
}

type Job =
  | { kind: "frames"; bufs: Buffer[] }
  | { kind: "snapshot"; term: number; index: number; lastTerm: number };

class Peer {
  sock?: net.Socket;
  sess?: Session;
  ready = false;
  streaming = false;
  queue: Job[] = [];
  backoff = 20;
  timer?: NodeJS.Timeout;
  constructor(readonly id: NodeId, readonly addr: string) {}
}

export class Transport {
  readonly stats = new NetStats();
  private readonly ids: NodeId[];
  private readonly peers = new Map<NodeId, Peer>();
  private readonly server = net.createServer();
  private readonly socks = new Set<net.Socket>();
  private stopped = false;
  private readonly mine: string;

  constructor(private readonly me: NodeId, members: Member[], private readonly clusterId: number,
    private readonly sink: (i: Inbound) => void, private readonly snaps: SnapStore, private readonly filter: Filter,
    private readonly key?: Uint8Array) {
    this.ids = members.map((m) => m.id);
    this.mine = members.find((m) => m.id === me)!.addr;
    for (const m of members) if (m.id !== me) this.peers.set(m.id, new Peer(m.id, m.addr));
  }

  /// Bind the listener and start connecting to peers.
  async start(): Promise<void> {
    const { host, port } = parseAddr(this.mine);
    await new Promise<void>((res, rej) => {
      this.server.once("error", rej);
      this.server.listen(port, host, () => { this.server.off("error", rej); res(); });
    });
    this.server.on("connection", (s) => this.accept(s));
    for (const p of this.peers.values()) this.connect(p);
  }

  async shutdown(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    for (const p of this.peers.values()) if (p.timer) clearTimeout(p.timer);
    for (const s of this.socks) s.destroy();
    await new Promise<void>((res) => this.server.close(() => res()));
  }

  // ---- sending ----

  private enqueue(to: NodeId, job: Job): void {
    if (this.stopped) return;
    if (!this.filter.allows(this.me, to)) { this.stats.dropped++; return; }
    const p = this.peers.get(to);
    if (!p) return;
    if (!p.ready) { this.stats.dropped++; return; } // down: shed, the protocol retries
    if (p.streaming || job.kind === "snapshot") {
      if (p.queue.length >= PEER_QUEUE) { this.stats.dropped++; return; }
      p.queue.push(job);
      if (!p.streaming) this.drain(p);
      return;
    }
    this.write(p, job.bufs);
  }

  private write(p: Peer, bufs: Buffer[]): void {
    const s = p.sock!;
    if (s.writableLength > MAX_SOCKET_BACKLOG) { this.stats.dropped++; return; }
    this.stats.sent++;
    for (const b of bufs) s.write(b);
  }

  send(to: NodeId, m: Msg): void {
    const p = this.peers.get(to);
    if (!p || !p.ready || this.stopped) { if (p && !this.stopped) this.stats.dropped++; return; }
    if (!this.filter.allows(this.me, to)) { this.stats.dropped++; return; }
    const frame = putFrame(K_MSG, encodeMsg(m).done(), p.sess);
    if (p.streaming) { this.enqueue(to, { kind: "frames", bufs: [frame] }); return; }
    this.write(p, [frame]);
  }

  sendDigest(to: NodeId, index: number, d: Uint8Array): void {
    const p = this.peers.get(to);
    if (!p || !p.ready) return;
    const body = new Out(40).u64(index).bytes(d).done();
    this.enqueue(to, { kind: "frames", bufs: [putFrame(K_DIGEST, body, p.sess)] });
  }

  /// Stream the stored snapshot at `index` to a peer.
  sendSnapshot(to: NodeId, term: number, index: number, lastTerm: number): void {
    this.enqueue(to, { kind: "snapshot", term, index, lastTerm });
  }

  /// Run queued jobs for a peer, snapshots one at a time, in order.
  private drain(p: Peer): void {
    p.streaming = true;
    void (async () => {
      try {
        while (p.queue.length > 0 && p.ready && !this.stopped) {
          const j = p.queue.shift()!;
          if (j.kind === "frames") {
            // Frames queued behind a stream were sealed in enqueue order, which is write order.
            if (this.filter.allows(this.me, p.id)) this.write(p, j.bufs);
          } else {
            await this.stream(p, j);
          }
        }
      } catch {
        p.sock?.destroy();
      } finally {
        p.streaming = false;
      }
    })();
  }

  private async stream(p: Peer, j: { term: number; index: number; lastTerm: number }): Promise<void> {
    const snap = this.snaps.get(j.index);
    if (!snap) return; // gone (superseded): the peer will ask again
    const s = p.sock!;
    const flow = async (b: Buffer) => {
      if (!s.write(b)) await new Promise<void>((res) => { s.once("drain", res); s.once("close", res); });
      if (s.destroyed) throw new Error("connection lost");
    };
    const begin = new Out(32).u32(j.term).u64(j.index).u32(j.lastTerm).u64(snap.meta.len).u32(snap.meta.crc).done();
    await flow(putFrame(K_SNAP_BEGIN, begin, p.sess));
    for (let at = 0; at < snap.data.length; at += SNAP_CHUNK)
      await flow(putFrame(K_SNAP_CHUNK, snap.data.subarray(at, Math.min(at + SNAP_CHUNK, snap.data.length)), p.sess));
    await flow(putFrame(K_SNAP_END, Buffer.alloc(0), p.sess));
    this.stats.sent++;
  }

  // ---- outbound connections ----

  private helloBody(): Out {
    return new Out(32).u8(this.me).u64(this.clusterId);
  }

  private connect(p: Peer): void {
    if (this.stopped) return;
    const { host, port } = parseAddr(p.addr);
    const s = net.connect({ host, port });
    s.setNoDelay(true);
    this.socks.add(s);
    let up = false;
    const dial = setTimeout(() => s.destroy(), 300);
    const lost = () => {
      clearTimeout(dial);
      this.socks.delete(s);
      if (p.sock === s) {
        p.ready = false;
        p.sock = undefined;
        p.sess = undefined;
        p.queue = [];
        p.streaming = false;
      }
      if (up) p.backoff = 20;
      if (!this.stopped) {
        p.timer = setTimeout(() => this.connect(p), p.backoff);
        p.backoff = Math.min(p.backoff * 2, 500);
      }
    };
    s.on("error", () => s.destroy());
    s.on("close", lost);
    s.on("connect", () => {
      clearTimeout(dial);
      up = true;
      p.sock = s;
      if (!this.key) {
        s.write(putFrame(K_HELLO, this.helloBody().done()));
        p.ready = true;
        s.on("data", () => {}); // nothing is expected back
        return;
      }
      const ns = nonce();
      s.write(putFrame(K_HELLO, this.helloBody().bytes(ns).done()));
      const timer = setTimeout(() => { this.stats.badFrames++; s.destroy(); }, 2000);
      const reader = new FrameReader();
      s.on("data", (chunk: Buffer) => {
        if (p.ready) return;
        try {
          reader.push(chunk, (f) => this.onChallenge(p, s, ns, f, timer));
        } catch {
          this.stats.badFrames++;
          s.destroy();
        }
      });
    });
  }

  private onChallenge(p: Peer, s: net.Socket, ns: Buffer, f: Frame, timer: NodeJS.Timeout): void {
    clearTimeout(timer);
    if (f.kind !== K_CHALLENGE || f.body.length !== NONCE_LEN + MAC_LEN) throw new NetError("challenge");
    const ids = Buffer.from([this.me, p.id]);
    const theirs = f.body.subarray(0, NONCE_LEN);
    const want = handshake(this.key!, "chal", ns, theirs, ids);
    if (!ctEq(want.subarray(0, MAC_LEN), f.body.subarray(NONCE_LEN))) throw new NetError("challenge mac");
    const sess = new Session(handshake(this.key!, "sess", ns, theirs, ids), 0);
    s.write(putFrame(K_AUTH, Buffer.alloc(0), sess)); // proves this side knows the key too
    p.sess = sess;
    p.ready = true;
  }

  // ---- inbound connections ----

  private accept(s: net.Socket): void {
    if (this.stopped) { s.destroy(); return; }
    s.setNoDelay(true);
    this.socks.add(s);
    s.on("close", () => this.socks.delete(s));
    s.on("error", () => s.destroy());
    const bad = () => { this.stats.badFrames++; };
    const reader = new FrameReader();
    let stage: "hello" | "auth" | "stream" = "hello";
    let from: NodeId = 0;
    let incoming: Incoming | undefined;
    let inTerm = 0, inLast = 0, inIndex = 0;
    let hello = setTimeout(() => s.destroy(), 5000);
    s.on("close", () => { clearTimeout(hello); incoming?.abort(); });
    const fail = (counted: boolean) => { if (counted) bad(); s.destroy(); };

    const onFrame = (f: Frame): void => {
      if (stage === "hello") {
        clearTimeout(hello);
        const want = this.key ? 9 + NONCE_LEN : 9;
        if (f.kind !== K_HELLO || f.body.length !== want) throw new NetError("hello");
        from = f.body[0];
        const cid = f.body.readUInt32LE(1) + f.body.readUInt32LE(5) * 4294967296;
        if (cid !== this.clusterId || from === this.me || !this.ids.includes(from)) throw new NetError("hello");
        if (!this.key) { stage = "stream"; return; }
        const ns = f.body.subarray(9);
        const nr = nonce();
        const ids = Buffer.from([from, this.me]);
        const mac = handshake(this.key, "chal", ns, nr, ids);
        s.write(putFrame(K_CHALLENGE, Buffer.concat([nr, mac.subarray(0, MAC_LEN)])));
        reader.sess = new Session(handshake(this.key, "sess", ns, nr, ids), 1);
        stage = "auth";
        hello = setTimeout(() => { bad(); s.destroy(); }, 2000);
        return;
      }
      if (stage === "auth") {
        clearTimeout(hello);
        if (f.kind !== K_AUTH || f.body.length !== 0) throw new NetError("auth");
        stage = "stream";
        return;
      }
      if (!this.filter.allows(from, this.me)) return;
      this.stats.received++;
      const r = new Reader(f.body);
      switch (f.kind) {
        case K_MSG: {
          const msg = decodeMsg(f.body);
          this.sink({ kind: "msg", from, msg });
          break;
        }
        case K_DIGEST: {
          const index = r.u64();
          const digest = Buffer.from(r.bytes(32));
          this.sink({ kind: "digest", from, index, digest });
          break;
        }
        case K_SNAP_BEGIN: {
          inTerm = r.u32(); inIndex = r.u64(); inLast = r.u32();
          const len = r.u64(), crc = r.u32();
          incoming?.abort();
          incoming = this.snaps.receive(inIndex, inLast, len, crc);
          break;
        }
        case K_SNAP_CHUNK:
          if (!incoming) throw new NetError("chunk without begin");
          incoming.write(f.body);
          break;
        case K_SNAP_END: {
          if (!incoming) throw new NetError("end without begin");
          const inc = incoming;
          incoming = undefined;
          inc.finish();
          this.sink({ kind: "snapshot", from, term: inTerm, index: inIndex, lastTerm: inLast });
          break;
        }
        default: throw new NetError("frame kind");
      }
    };

    s.on("data", (chunk: Buffer) => {
      try {
        reader.push(chunk, onFrame);
      } catch (e) {
        fail(e instanceof NetError || e instanceof CodecError);
      }
    });
  }
}
