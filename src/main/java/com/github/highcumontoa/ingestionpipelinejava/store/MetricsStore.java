package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.MetricSnapshot;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * 每来源接收/写入/拒绝/重试计数（内存原子累加 + 变更后落盘 metrics.json）。
 * 失败计数按原因归类键记录，rejected 为鉴权/参数等批次级拒绝总数。
 */
@Component
public class MetricsStore {

    /** 计数包；字段公开给同包写入器使用。 */
    public static class Counters {
        public final AtomicLong received = new AtomicLong();
        public final AtomicLong accepted = new AtomicLong();
        public final AtomicLong duplicate = new AtomicLong();
        public final AtomicLong conflict = new AtomicLong();
        public final AtomicLong quarantined = new AtomicLong();
        public final AtomicLong rejected = new AtomicLong();
        /** 容量治理细分：被进程级总容量拦下 */
        public final AtomicLong rejectedGlobal = new AtomicLong();
        /** 容量治理细分：被该来源自身在途积压上限拦下 */
        public final AtomicLong rejectedBacklog = new AtomicLong();
        /** 容量治理细分：被该来源自身速率配额拦下 */
        public final AtomicLong rejectedRate = new AtomicLong();
        public final AtomicLong retried = new AtomicLong();
        public final AtomicLong dlqPending = new AtomicLong();
        public final AtomicLong dlqExhausted = new AtomicLong();
    }

    private final Path root;
    private final Map<String, Counters> counters = new ConcurrentHashMap<>();

    public MetricsStore(PipelineProperties props) {
        this.root = props.getStateDir().resolve("sources");
    }

    private Path file(String source) {
        return root.resolve(source).resolve("metrics.json");
    }

    public Counters forSource(String source) {
        return counters.computeIfAbsent(source, k -> {
            Counters c = new Counters();
            var loaded = JsonFiles.read(file(k),
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Long>>() {
                    });
            if (loaded != null) {
                apply(c.received, loaded.get("received"));
                apply(c.accepted, loaded.get("accepted"));
                apply(c.duplicate, loaded.get("duplicate"));
                apply(c.conflict, loaded.get("conflict"));
                apply(c.quarantined, loaded.get("quarantined"));
                apply(c.rejected, loaded.get("rejected"));
                apply(c.rejectedGlobal, loaded.get("rejectedGlobal"));
                apply(c.rejectedBacklog, loaded.get("rejectedBacklog"));
                apply(c.rejectedRate, loaded.get("rejectedRate"));
                apply(c.retried, loaded.get("retried"));
                apply(c.dlqPending, loaded.get("dlqPending"));
                apply(c.dlqExhausted, loaded.get("dlqExhausted"));
            }
            return c;
        });
    }

    private static void apply(AtomicLong a, Long v) {
        if (v != null) {
            a.set(v);
        }
    }

    /** 快照并落盘（调用频率有界：按批次写一次）。 */
    public MetricSnapshot snapshot(String source, long offset, int inflight) {
        Counters c = forSource(source);
        MetricSnapshot s = new MetricSnapshot(source, c.received.get(), c.accepted.get(),
                c.duplicate.get(), c.conflict.get(), c.quarantined.get(), c.rejected.get(),
                c.retried.get(), c.dlqPending.get(), c.dlqExhausted.get(), offset, inflight);
        JsonFiles.writeAtomically(file(source), Map.ofEntries(
                Map.entry("received", s.received()),
                Map.entry("accepted", s.accepted()),
                Map.entry("duplicate", s.duplicate()),
                Map.entry("conflict", s.conflict()),
                Map.entry("quarantined", s.quarantined()),
                Map.entry("rejected", s.rejected()),
                Map.entry("rejectedGlobal", c.rejectedGlobal.get()),
                Map.entry("rejectedBacklog", c.rejectedBacklog.get()),
                Map.entry("rejectedRate", c.rejectedRate.get()),
                Map.entry("retried", s.retried()),
                Map.entry("dlqPending", s.dlqPending()),
                Map.entry("dlqExhausted", s.dlqExhausted())));
        return s;
    }

    public MetricSnapshot inMemorySnapshot(String source, long offset, int inflight) {
        Counters c = forSource(source);
        return new MetricSnapshot(source, c.received.get(), c.accepted.get(),
                c.duplicate.get(), c.conflict.get(), c.quarantined.get(), c.rejected.get(),
                c.retried.get(), c.dlqPending.get(), c.dlqExhausted.get(), offset, inflight);
    }

    /** 以 DLQ 当前真实状态为准同步两个存量计数，避免增量增减漂移。 */
    public void refreshDlqCounts(String source, long pending, long exhausted) {
        Counters c = forSource(source);
        c.dlqPending.set(pending);
        c.dlqExhausted.set(exhausted);
    }
}
