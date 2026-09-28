package com.github.highcumontoa.ingestionpipelinejava.service.pull;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceType;
import com.github.highcumontoa.ingestionpipelinejava.model.DeadLetter;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceDefinition;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 拉取调度：为每个 PULL_* 来源周期轮询并串行处理。
 * <ul>
 *   <li>位点连续确认：每条成功/隔离后 expected=当前值、next=+1 提交；重启从上次位点继续；</li>
 *   <li>回退/非连续 -> OffsetStore 拒绝；源端缺口/不可用 -> 冻结该来源（OFFSET_FROZEN），不跳位；</li>
 *   <li>限速：令牌不足则结束本轮等待下次；在途与积压策略复用 RateLimiter；</li>
 *   <li>单条坏数据隔离到 DLQ，不中断整轮。</li>
 * </ul>
 */
@Component
public class PullCoordinator {

    private static final Logger log = LoggerFactory.getLogger(PullCoordinator.class);

    private final SourceRegistry registry;
    private final IngestionService ingestion;
    private final OffsetStore offsets;
    private final DeadLetterStore dlq;
    private final RateLimiter rateLimiter;
    private final MetricsStore metrics;
    private final com.github.highcumontoa.ingestionpipelinejava.service.QuotaService quotaService;
    private final com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor capacity;
    private final boolean enabled;

    private final Map<String, SourceReader> readers = new ConcurrentHashMap<>();
    private final Map<String, Boolean> frozen = new ConcurrentHashMap<>();
    private final Map<String, Boolean> running = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor;

    /** 兼容构造（既有单测直接装配）：不启用进程级总容量，配额取注册表静态定义。 */
    public PullCoordinator(SourceRegistry registry, IngestionService ingestion,
                           OffsetStore offsets, DeadLetterStore dlq, RateLimiter rateLimiter,
                           MetricsStore metrics, boolean enabled) {
        this(registry, ingestion, offsets, dlq, rateLimiter, metrics,
                new com.github.highcumontoa.ingestionpipelinejava.service.QuotaService(
                        registry, rateLimiter),
                new com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor(0, 1),
                enabled);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public PullCoordinator(SourceRegistry registry, IngestionService ingestion,
                           OffsetStore offsets, DeadLetterStore dlq, RateLimiter rateLimiter,
                           MetricsStore metrics,
                           com.github.highcumontoa.ingestionpipelinejava.service.QuotaService quotaService,
                           com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor capacity,
                           @Value("${ingestion.pull.enabled:true}") boolean enabled) {
        this.registry = registry;
        this.ingestion = ingestion;
        this.offsets = offsets;
        this.dlq = dlq;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
        this.quotaService = quotaService;
        this.capacity = capacity;
        this.enabled = enabled;
    }

    @PostConstruct
    public void start() {
        List<SourceDefinition> pullSources = registry.all().stream()
                .filter(d -> d.type() != SourceType.PUSH)
                .toList();
        for (SourceDefinition d : pullSources) {
            SourceReader reader = d.type() == SourceType.PULL_FILE
                    ? new FileSourceReader(d.dir())
                    : new HttpSourceReader(d.url(), d.token());
            readers.put(d.name(), reader);
            // 限速/在途桶由 QuotaService 在启动时对所有来源（推送+拉取）统一播种，
            // 拉取调度不再自行注册，避免只覆盖拉取来源的启动期缺口。
            frozen.put(d.name(), Boolean.FALSE);
            running.put(d.name(), Boolean.FALSE);
        }
        if (!enabled || pullSources.isEmpty()) {
            log.info("pull coordinator not scheduled (enabled={}, pullSources={})", enabled, pullSources.size());
            return;
        }
        executor = Executors.newScheduledThreadPool(Math.max(1, pullSources.size()), r -> {
            Thread t = new Thread(r, "pull-coordinator");
            t.setDaemon(true);
            return t;
        });
        for (SourceDefinition d : pullSources) {
            long period = Math.max(50, d.pollIntervalMs());
            executor.scheduleWithFixedDelay(() -> safeTick(d.name()), period, period, TimeUnit.MILLISECONDS);
            log.info("pull scheduled source={} type={} periodMs={} resumeOffset={}",
                    d.name(), d.type(), period, offsets.current(d.name()));
        }
    }

    private void safeTick(String source) {
        try {
            tick(source);
        } catch (Throwable t) {
            log.error("pull tick failed source={}: {}", source, t.toString());
        }
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public boolean isFrozen(String source) {
        return frozen.getOrDefault(source, Boolean.FALSE);
    }

    /** 测试/扩展用：替换某来源的读取器（例如注入一个制造缺口的读取器）。 */
    public void registerReader(String source, SourceReader reader) {
        readers.put(source, reader);
    }

    /** 手工解冻（修复源端缺口后恢复）。 */
    public void unfreeze(String source) {
        registry.require(source);
        frozen.put(source, Boolean.FALSE);
        log.info("source unfrozen: {}", source);
    }

    /**
     * 对单个来源执行一轮拉取。
     *
     * @return 本轮推进的记录数；冻结/限速/无数据时为 0
     */
    public int tick(String source) {
        SourceDefinition def = registry.require(source);
        if (def.type() == SourceType.PUSH) {
            return 0;
        }
        // 同来源串行：上一轮未结束则跳过，避免并发推进导致越位/错位
        synchronized (running) {
            if (Boolean.TRUE.equals(running.get(source))) {
                return 0;
            }
            running.put(source, Boolean.TRUE);
        }
        try {
            if (Boolean.TRUE.equals(frozen.get(source))) {
                log.warn("source '{}' is frozen (previous gap/unavailable); skip tick", source);
                return 0;
            }
            // 进程级总容量：拉取与推送共用同一份总量；到顶即本轮明确放弃（数据留在源端不丢）
            if (!capacity.tryAcquire(source)) {
                metrics.forSource(source).rejected.incrementAndGet();
                metrics.forSource(source).rejectedGlobalCapacity.incrementAndGet();
                log.warn("source '{}' blocked by global capacity; skip tick (bounded rejection)", source);
                return 0;
            }
            try {
            if (!rateLimiter.tryAcquireInflight(source)) {
                metrics.forSource(source).rejected.incrementAndGet();
                metrics.forSource(source).rejectedBacklogFull.incrementAndGet();
                log.warn("source '{}' backlog full; skip tick (bounded rejection)", source);
                return 0;
            }
            try {
                long current = offsets.current(source);
                SourceReader reader = readers.computeIfAbsent(source, k ->
                        def.type() == SourceType.PULL_FILE
                                ? new FileSourceReader(def.dir())
                                : new HttpSourceReader(def.url(), def.token()));
                List<SourceReader.PulledRecord> batch;
                try {
                    // 单批规模取运行期生效配额（热调整后立即对后续轮次生效）
                    batch = reader.read(current, quotaService.effective(source).batchSize());
                } catch (IngestionException e) {
                    if (e.reason() == RejectReason.SOURCE_UNAVAILABLE) {
                        freeze(source, e.getMessage());
                        return 0;
                    }
                    throw e;
                }
                if (batch.isEmpty()) {
                    return 0;
                }
                long expectedFirst = current + 1;
                if (batch.get(0).sequence() != expectedFirst) {
                    freeze(source, "non-contiguous sequence at resume point: expected "
                            + expectedFirst + " got " + batch.get(0).sequence());
                    return 0;
                }
                int processed = 0;
                for (SourceReader.PulledRecord pr : batch) {
                    long seq = pr.sequence();
                    if (seq != current + 1) {
                        freeze(source, "sequence gap: expected " + (current + 1) + " got " + seq);
                        break;
                    }
                    if (!rateLimiter.tryAcquireToken(source, 1)) {
                        log.info("rate limited mid-batch source={} at seq={}", source, seq);
                        break;
                    }
                    processRecord(source, def, seq, pr);
                    // 只有正确到达的下一条才提交；OffsetStore 二次保证不回退/不跳跃
                    offsets.commitContiguous(source, current, seq, true);
                    current = seq;
                    processed++;
                }
                return processed;
            } finally {
                rateLimiter.releaseInflight(source);
            }
            } finally {
                capacity.release(source);
            }
        } finally {
            running.put(source, Boolean.FALSE);
        }
    }

    private void processRecord(String source, SourceDefinition def, long seq,
                               SourceReader.PulledRecord pr) {
        if (pr.parseError() != null) {
            DeadLetter d = dlq.add(source, seq, null, RejectReason.MALFORMED_BODY,
                    pr.parseError(), 1);
            d.setStatus(DeadLetter.Status.EXHAUSTED);
            dlq.update(source, x -> x == d, x -> x.setStatus(DeadLetter.Status.EXHAUSTED));
            metrics.forSource(source).quarantined.incrementAndGet();
            metrics.refreshDlqCounts(source,
                    dlq.countByStatus(source, DeadLetter.Status.PENDING),
                    dlq.countByStatus(source, DeadLetter.Status.EXHAUSTED));
            log.warn("malformed source line quarantined source={} seq={}", source, seq);
            return;
        }
        IncomingRecord record = pr.record();
        DeadLetter failed = ingestion.processOne(source, seq, record, 1);
        if (failed != null && failed.getAttempts() >= def.maxAttempts()) {
            // 结构类坏数据：保留隔离即可，位点继续，保证整批不被单条坏数据中断
            log.warn("record exhausted into DLQ source={} seq={} key={}", source, seq, record.key());
        }
    }

    private void freeze(String source, String why) {
        frozen.put(source, Boolean.TRUE);
        log.error("source '{}' FROZEN: {} (offset stays at {}, no data written beyond it)",
                source, why, offsets.current(source));
        throw new IngestionException(RejectReason.OFFSET_FROZEN,
                "source '" + source + "' frozen: " + why);
    }
}
