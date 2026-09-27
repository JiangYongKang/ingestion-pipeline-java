package com.github.highcumontoa.ingestionpipelinejava.model;

/**
 * 某来源的可观测计数快照。
 * rejected 为批次级拒绝总数；rejectedRateLimited / rejectedBacklogFull /
 * rejectedGlobalCapacity 按拒绝原因细分（该来源自身限速 / 自身积压 / 进程级总容量），
 * 三者之和等于因容量类原因被拒的次数，可据此区分"总容量顶满"与"自身配额先用超"。
 */
public record MetricSnapshot(String source, long received, long accepted, long duplicate,
                             long conflict, long quarantined, long rejected, long retried,
                             long dlqPending, long dlqExhausted, long offset, int inflight,
                             long rejectedRateLimited, long rejectedBacklogFull,
                             long rejectedGlobalCapacity) {
}
