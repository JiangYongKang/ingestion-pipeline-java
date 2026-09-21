package com.github.highcumontoa.ingestionpipelinejava.model;

/** 某来源的可观测计数快照。 */
public record MetricSnapshot(String source, long received, long accepted, long duplicate,
                             long conflict, long quarantined, long rejected, long retried,
                             long dlqPending, long dlqExhausted, long offset, int inflight) {
}
