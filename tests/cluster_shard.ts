// Scale-out: ShardMap assignment (checked against values computed
// independently from the splitmix64 definition, which the Rust, C++ and Go
// maps share), ShardedClient, and two groups end to end.
//
//   node dist/tests/cluster_shard.js
import { Client, ClientServer, CmdSym, ShardMap, ShardedClient, decodeBatch, encodeBatch } from "../src/cluster";
import { hashPartition } from "../src/routing";
import { newLimit, Side, Tif } from "../src/matcher/types";
import { check, runAll, runAllAsync } from "./t";
import { TestCluster, freeAddrs } from "./cluster_util";

const batchOf = (next: { n: number }, syms: number[], n: number): CmdSym[] =>
  Array.from({ length: n }, (_, i) => [syms[i % syms.length], newLimit(++next.n, Side.Bid, 10000, 1, Tif.Gtc)] as CmdSym);

/// A replicated group with client ports.
class Group {
  servers: ClientServer[] = [];
  addrs: string[] = [];
  private constructor(readonly c: TestCluster) {}
  static async create(cluster: number): Promise<Group> {
    const g = new Group(await TestCluster.create(3, { dedup: true, tune: (cfg) => (cfg.clusterId = cluster) }));
    g.addrs = await freeAddrs(3);
    for (let i = 0; i < 3; i++) g.servers.push(await ClientServer.listen(g.addrs[i], g.c.nodes[i]!));
    return g;
  }
  client(session: number): Client {
    const c = new Client(this.addrs.map((addr, id) => ({ id, addr }))).withSession(session);
    c.retryBudgetMs = 3000;
    return c;
  }
  async restartNode(i: number): Promise<void> {
    await this.servers[i].shutdown();
    await this.c.restart(i);
    this.servers[i] = await ClientServer.listen(this.addrs[i], this.c.nodes[i]!);
  }
  applied(node: number): CmdSym[] {
    return this.c.shared[node].payloads().flatMap((p) => decodeBatch(p));
  }
  async close(): Promise<void> {
    await Promise.all(this.servers.map((s) => s.shutdown()));
    await this.c.close();
  }
}

const sync: Array<[string, () => void]> = [
  ["group assignment matches the other languages", () => {
    // Computed independently (Python) from the splitmix64 definition.
    const syms = [0, 1, 2, 3, 7, 100, 4095, 65535, 4294967295];
    const want: Record<number, number[]> = { 4: [3, 1, 2, 1, 3, 0, 3, 2, 0], 3: [1, 2, 1, 0, 0, 2, 2, 1, 2] };
    for (const g of [4, 3]) {
      const m = new ShardMap(g);
      syms.forEach((s, i) => check(m.groupOf(s) === want[g][i], "groups=", g, " sym=", s, ": ", m.groupOf(s), ", want ", want[g][i]));
    }
  }],

  ["every symbol has one stable group and groups are balanced", () => {
    const m = new ShardMap(4);
    const counts = [0, 0, 0, 0];
    for (let s = 0; s < 40000; s++) {
      const g = m.groupOf(s);
      check(g === m.groupOf(s), "unstable");
      counts[g]++;
    }
    check(counts.every((c) => c >= 9000 && c <= 11000), "unbalanced: ", counts.join());
  }],

  ["a group's symbols still spread over its partitions", () => {
    // If the group hash correlated with the pipeline's partition hash, a
    // group's symbols would pile onto one partition.
    const m = new ShardMap(2);
    const grid = [[0, 0], [0, 0]];
    for (let s = 0; s < 20000; s++) grid[m.groupOf(s)][hashPartition(s, 2)]++;
    check(grid.flat().every((c) => c >= 4000 && c <= 6000), "skewed: ", JSON.stringify(grid));
  }],

  ["split keeps each symbol's order and loses nothing", () => {
    const m = new ShardMap(3);
    const cmds: CmdSym[] = Array.from({ length: 300 }, (_, i) => [i % 17, newLimit(i + 1, Side.Bid, 100, 1, Tif.Gtc)] as CmdSym);
    let total = 0;
    m.split(cmds).forEach((part, g) => {
      total += part.length;
      let prev = 0;
      for (const [sym, c] of part) {
        check(m.groupOf(sym) === g, "command in the wrong group");
        check(c.orderId > prev, "order within a group changed");
        prev = c.orderId;
      }
    });
    check(total === cmds.length, "split lost commands: ", total);
  }],
];

const async_: Array<[string, () => Promise<void>]> = [
  ["groups hold only their symbols in per-symbol order", async () => {
    const g = [await Group.create(11), await Group.create(12)];
    try {
      for (const x of g) await x.c.waitLeader();
      const sc = new ShardedClient([g[0].client(1), g[1].client(1)]);
      const syms = Array.from({ length: 12 }, (_, i) => i);
      const next = { n: 0 };
      const sent = new Map<number, number[]>();
      for (let i = 0; i < 40; i++) {
        const b = batchOf(next, syms, 25);
        for (const [s, c] of b) sent.set(s, [...(sent.get(s) ?? []), c.orderId]);
        (await sc.propose(b)).forEach((o, gi) => check(!o.sent || o.result.kind === "ok", "group ", gi, ": ", o.result.kind));
      }
      for (let gi = 0; gi < 2; gi++) {
        await g[gi].c.waitConverged(10000);
        const got = new Map<number, number[]>();
        for (const [s, c] of g[gi].applied(0)) {
          check(sc.map.groupOf(s) === gi, "group ", gi, " holds symbol ", s, " of another group");
          got.set(s, [...(got.get(s) ?? []), c.orderId]);
        }
        for (const [s, ids] of got) check(JSON.stringify(ids) === JSON.stringify(sent.get(s)), "group ", gi, " symbol ", s, ": order or content differs");
      }
      sc.close();
    } finally {
      for (const x of g) await x.close();
    }
  }],

  ["an outage in one group does not stop the other", async () => {
    const g = [await Group.create(21), await Group.create(22)];
    try {
      for (const x of g) await x.c.waitLeader();
      const m = new ShardMap(2);
      const mine: number[][] = [[], []];
      for (let s = 0; mine[0].length < 4 || mine[1].length < 4; s++) mine[m.groupOf(s)].push(s);
      for (let i = 0; i < 3; i++) await g[1].c.crash(i); // group 1 goes down entirely
      const c0 = g[0].client(1);
      const next = { n: 0 };
      for (let i = 0; i < 50; i++) check((await c0.propose(encodeBatch(batchOf(next, mine[0].slice(0, 4), 10)))).kind === "ok", "healthy group failed while its sibling was down");
      for (let i = 0; i < 3; i++) await g[1].restartNode(i);
      await g[1].c.waitLeader(10000);
      const c1 = g[1].client(2);
      check((await c1.propose(encodeBatch(batchOf(next, mine[1].slice(0, 4), 10)))).kind === "ok", "restarted group");
      c0.close();
      c1.close();
    } finally {
      for (const x of g) await x.close();
    }
  }],
];

runAll(sync);
const syncFailed = process.exitCode;
runAllAsync(async_).then(() => { if (syncFailed) process.exitCode = syncFailed; });
