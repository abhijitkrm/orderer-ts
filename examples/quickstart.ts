// The README quick start, runnable: node dist/examples/quickstart.js
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Pipeline, collect, journalConfig, matcher } from "../src/index";

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "orderer-quickstart-"));
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
process.stdout.write(listing.listing() + fs.readFileSync(path.join(dir, "books.snap"), "utf8"));
fs.rmSync(dir, { recursive: true, force: true });
