//! Operational statistics (not part of the spec contract): ring depths,
//! per-partition counters, journal watermarks and fsync timings, and a
//! Prometheus text-format rendering.

export interface PartitionStats {
  partition: number;
  inboxDepth: number; // routed, not yet applied
  outboxDepth: number; // staged, not yet consumed by egress
  commands: number;
  events: number;
  flushedIseq: number; // Infinity without journals
  durableIseq: number;
  fsyncs: number;
  fsyncNsTotal: number;
  fsyncNsMax: number;
}

/// A point-in-time view (fields individually exact, not mutually consistent).
export interface PipelineStats {
  ingressDepth: number; // published, not yet routed
  partitions: PartitionStats[];
}

/// Prometheus text exposition format (version 0.0.4).
export function toPrometheus(st: PipelineStats): string {
  let out = "# HELP orderer_ingress_depth Commands published, not yet routed.\n# TYPE orderer_ingress_depth gauge\n" +
    `orderer_ingress_depth ${st.ingressDepth}\n`;
  const series = (name: string, help: string, kind: string, get: (p: PartitionStats) => number) => {
    out += `# HELP orderer_${name} ${help}\n# TYPE orderer_${name} ${kind}\n`;
    for (const p of st.partitions) out += `orderer_${name}{partition="${p.partition}"} ${get(p)}\n`;
  };
  series("inbox_depth", "Commands routed, not yet applied.", "gauge", (p) => p.inboxDepth);
  series("outbox_depth", "Events staged, not yet consumed by egress.", "gauge", (p) => p.outboxDepth);
  series("commands_total", "Commands applied.", "counter", (p) => p.commands);
  series("events_total", "Events emitted.", "counter", (p) => p.events);
  series("durable_iseq", "Highest iseq covered by a completed fsync.", "gauge", (p) => p.durableIseq);
  series("fsyncs_total", "Journal fsyncs.", "counter", (p) => p.fsyncs);
  series("fsync_ns_total", "Time spent in journal fsync, in nanoseconds.", "counter", (p) => p.fsyncNsTotal);
  series("fsync_max_ns", "Longest journal fsync, in nanoseconds.", "gauge", (p) => p.fsyncNsMax);
  return out;
}
