package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.DeadLetter;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordResult;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.store.BatchIdempotencyStore;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 核心处理管线。调用方（控制器/拉取调度）必须先完成鉴权。
 * 顺序：批次幂等 -> 积压/限速 -> 结构声明 -> 逐条“校验隔离 + 幂等写入”。
 * 单条坏数据只隔离该条，绝不中断整批；未授权请求在进入本服务前已被拒绝。
 */
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final SourceRegistry registry;
    private final SchemaValidator schemaValidator;
    private final SchemaStore schemas;
    private final RecordSink sink;
    private final BatchIdempotencyStore batches;
    private final DeadLetterStore dlq;
    private final MetricsStore metrics;
    private final RateLimiter rateLimiter;
    private final QuotaService quotaService;
    private final CapacityGovernor capacity;

    /**
     * 兼容构造（既有单测直接装配）：不启用进程级总容量，配额取注册表静态定义。
     * 生产装配走下方 @Autowired 构造。
     */
    public IngestionService(SourceRegistry registry, SchemaValidator schemaValidator,
                            SchemaStore schemas, RecordSink sink, BatchIdempotencyStore batches,
                            DeadLetterStore dlq, MetricsStore metrics, RateLimiter rateLimiter) {
        this(registry, schemaValidator, schemas, sink, batches, dlq, metrics, rateLimiter,
                new QuotaService(registry, rateLimiter), new CapacityGovernor(0, 1));
    }

    @org.springframework.beans.factory.annotation.Autowired
    public IngestionService(SourceRegistry registry, SchemaValidator schemaValidator,
                            SchemaStore schemas, RecordSink sink, BatchIdempotencyStore batches,
                            DeadLetterStore dlq, MetricsStore metrics, RateLimiter rateLimiter,
                            QuotaService quotaService, CapacityGovernor capacity) {
        this.registry = registry;
        this.schemaValidator = schemaValidator;
        this.schemas = schemas;
        this.sink = sink;
        this.batches = batches;
        this.dlq = dlq;
        this.metrics = metrics;
        this.rateLimiter = rateLimiter;
        this.quotaService = quotaService;
        this.capacity = capacity;
    }

    /** 推送式批次接入（已鉴权）。 */
    public BatchResult ingestBatch(String source, RecordBatch batch) {
        SourceDefinition def = registry.require(source);
        if (batch == null || batch.records() == null) {
            throw bad(source, RejectReason.MALFORMED_BODY, "request body must contain 'records'");
        }
        if (batch.batchId() == null || batch.batchId().isBlank()) {
            throw bad(source, RejectReason.MISSING_BATCH_ID, "'batchId' is required for idempotent delivery");
        }
        int size = batch.records().size();
        QuotaService.Quota quota = quotaService.effective(source);
        if (size > quota.batchSize()) {
            throw bad(source, RejectReason.BATCH_TOO_LARGE,
                    "batch size " + size + " exceeds configured batch-size " + quota.batchSize());
        }
        // 整批重放：直接返回首次结果，不重复写入，也不占用/重复累计任何容量计数
        BatchResult replay = batches.get(source, batch.batchId());
        if (replay != null) {
            log.info("batch replay source={} batchId={} -> return first result", source, batch.batchId());
            metrics.forSource(source).received.addAndGet(size);
            return new BatchResult(replay.batchId(), true, replay.accepted(), replay.duplicate(),
                    replay.conflict(), replay.quarantined(), replay.records());
        }

        // 进程级总容量：任何来源都不得绕过；到顶即明确拒绝，不排队
        if (!capacity.tryAcquire(source)) {
            throw bad(source, RejectReason.GLOBAL_CAPACITY_EXHAUSTED,
                    "process-wide capacity exhausted (total in-flight limit reached for source '"
                            + source + "' fair share or global pool); reject according to policy");
        }
        try {
            if (!rateLimiter.tryAcquireInflight(source)) {
                throw bad(source, RejectReason.BACKLOG_FULL,
                        "source '" + source + "' inflight backlog is full (" + quota.maxInflight()
                                + "); reject according to policy");
            }
            try {
                metrics.forSource(source).received.addAndGet(size);
                if (!rateLimiter.tryAcquireToken(source, size)) {
                    throw bad(source, RejectReason.RATE_LIMITED,
                            "source '" + source + "' rate limit " + quota.rateLimitPerSec()
                                    + " records/s exceeded");
                }

            int schemaVersion;
            if (batch.schema() != null) {
                schemaVersion = schemaValidator.declare(source, batch.schema());
            } else {
                RecordSchema latest = schemas.latest(source);
                if (latest == null) {
                    throw bad(source, RejectReason.SCHEMA_VERSION_INVALID,
                            "source '" + source + "' has no schema; first batch must declare one");
                }
                schemaVersion = latest.version();
            }

            List<RecordResult> results = new ArrayList<>(size);
            int accepted = 0, duplicate = 0, conflict = 0, quarantined = 0;
            MetricsStore.Counters mc = metrics.forSource(source);
            for (IncomingRecord r : batch.records()) {
                RecordResult rr = handleOne(source, null, r, schemaVersion, def.maxAttempts(), 1);
                results.add(rr);
                switch (rr.status()) {
                    case ACCEPTED -> { accepted++; mc.accepted.incrementAndGet(); }
                    case DUPLICATE -> { duplicate++; mc.duplicate.incrementAndGet(); }
                    case CONFLICT -> { conflict++; mc.conflict.incrementAndGet(); }
                    case QUARANTINED -> { quarantined++; }
                }
            }
            BatchResult result = new BatchResult(batch.batchId(), false, accepted, duplicate,
                    conflict, quarantined, results);
            batches.save(source, batch.batchId(), result);
            log.info("batch done source={} batchId={} accepted={} duplicate={} conflict={} quarantined={}",
                    source, batch.batchId(), accepted, duplicate, conflict, quarantined);
                return result;
            } finally {
                rateLimiter.releaseInflight(source);
            }
        } finally {
            capacity.release(source);
        }
    }

    /**
     * 拉取路径 / 重试入口：处理单条记录（已鉴权上下文由调度器保证）。
     *
     * @param sequence 拉取位点记录（重试可为 null）
     * @param attempt  第几次尝试（从 1 起）
     * @return 被隔离的失败记录；null 表示接受（写入或幂等重复/冲突已标记）
     */
    public DeadLetter processOne(String source, Long sequence, IncomingRecord record, int attempt) {
        SourceDefinition def = registry.require(source);
        metrics.forSource(source).received.incrementAndGet();
        return processOneInternal(source, sequence, record, def.maxAttempts(), attempt);
    }

    private DeadLetter processOneInternal(String source, Long sequence, IncomingRecord record,
                                          int maxAttempts, int attempt) {
        Map<String, Object> detail = new LinkedHashMap<>();
        RejectReason reason = schemaValidator.validate(source, record, detail);
        if (reason != null) {
            // 与推送路径完全一致：隔离并累计 quarantined，单条坏数据不影响整批
            return quarantine(source, sequence, record, reason, detail, attempt, maxAttempts);
        }
        RecordSchema latest = schemas.latest(source);
        int version = latest == null ? 1 : latest.version();
        RecordResult rr = sink.put(source, record, version);
        MetricsStore.Counters mc = metrics.forSource(source);
        switch (rr.status()) {
            case ACCEPTED -> { mc.accepted.incrementAndGet(); return null; }
            case DUPLICATE -> { mc.duplicate.incrementAndGet(); return null; }
            case CONFLICT -> { mc.conflict.incrementAndGet();
                log.warn("content conflict source={} key={} detail={}", source, record.key(), rr.detail());
                return null; }
            default -> throw new IllegalStateException("unexpected status " + rr.status());
        }
    }

    /** push/pull 共用的逐记录“校验+写入”，返回记录级结果。 */
    private RecordResult handleOne(String source, Long sequence, IncomingRecord record,
                                   int schemaVersion, int maxAttempts, int attempt) {
        Map<String, Object> detail = new LinkedHashMap<>();
        RejectReason reason = schemaValidator.validate(source, record, detail);
        if (reason != null) {
            quarantine(source, sequence, record, reason, detail, attempt, maxAttempts);
            return RecordResult.quarantined(record.key(), reason, describe(reason, detail));
        }
        return sink.put(source, record, schemaVersion);
    }

    private DeadLetter quarantine(String source, Long sequence, IncomingRecord record,
                                  RejectReason reason, Map<String, Object> detail,
                                  int attempt, int maxAttempts) {
        String text = describe(reason, detail);
        log.warn("record quarantined source={} seq={} key={} reason={} detail={}",
                source, sequence, record == null ? null : record.key(), reason, detail);
        DeadLetter d = dlq.add(source, sequence, record, reason, text, attempt);
        MetricsStore.Counters mc = metrics.forSource(source);
        mc.quarantined.incrementAndGet(); // 每次隔离事件（含重试再失败）
        if (attempt >= maxAttempts) {
            d.setStatus(DeadLetter.Status.EXHAUSTED);
            dlq.update(source, x -> x == d, x -> x.setStatus(DeadLetter.Status.EXHAUSTED));
        }
        syncDlqGauges(source);
        return d;
    }

    /** dlqPending/dlqExhausted 为存量值，一律以 DLQ 真实状态为准。 */
    private void syncDlqGauges(String source) {
        metrics.refreshDlqCounts(source,
                dlq.countByStatus(source, DeadLetter.Status.PENDING),
                dlq.countByStatus(source, DeadLetter.Status.EXHAUSTED));
    }

    private static String describe(RejectReason reason, Map<String, Object> detail) {
        return reason + (detail.isEmpty() ? "" : " " + detail);
    }

    public RejectReason classify(Throwable t) {
        if (t instanceof IngestionException ie) {
            return ie.reason();
        }
        return RejectReason.SOURCE_UNAVAILABLE;
    }

    /** 对某来源 DLQ 中仍可重试的记录执行一轮重试；返回 [重入成功, 仍失败, 已耗尽]。 */
    public int[] retryPending(String source) {
        SourceDefinition def = registry.require(source);
        int resolved = 0, stillFailed = 0, exhausted = 0;
        for (DeadLetter d : dlq.pending(source)) {
            int attempt = d.getAttempts() + 1;
            metrics.forSource(source).retried.incrementAndGet();
            IncomingRecord r = new IncomingRecord(d.getKey(), d.getData());
            DeadLetter again;
            boolean failedByException = false;
            try {
                again = processOneInternal(source, d.getSequence(), r, def.maxAttempts(), attempt);
            } catch (Exception e) {
                failedByException = true;
                again = d;
                RejectReason reason = classify(e);
                DeadLetter stored = dlq.add(source, d.getSequence(), r, reason,
                        reason + " " + e.getMessage(), attempt);
                metrics.forSource(source).quarantined.incrementAndGet();
                if (attempt >= def.maxAttempts()) {
                    stored.setStatus(DeadLetter.Status.EXHAUSTED);
                    dlq.update(source, x -> x == stored,
                            x -> x.setStatus(DeadLetter.Status.EXHAUSTED));
                }
            }
            if (!failedByException && again == null) {
                final String key = d.getKey();
                final Long seq = d.getSequence();
                dlq.update(source, x -> {
                    if (seq != null && x.getSequence() != null) {
                        return seq.equals(x.getSequence());
                    }
                    return key != null && key.equals(x.getKey());
                }, x -> x.setStatus(DeadLetter.Status.RESOLVED));
                resolved++;
            } else if (attempt >= def.maxAttempts()) {
                exhausted++;
            } else {
                stillFailed++;
            }
        }
        syncDlqGauges(source);
        log.info("dlq retry source={} resolved={} stillFailed={} exhausted={}",
                source, resolved, stillFailed, exhausted);
        return new int[]{resolved, stillFailed, exhausted};
    }

    private IngestionException bad(String source, RejectReason reason, String msg) {
        MetricsStore.Counters c = metrics.forSource(source);
        c.rejected.incrementAndGet();
        // 容量类拒绝按原因细分：自身限速 / 自身积压 / 进程级总容量，可区分、可查询
        switch (reason) {
            case RATE_LIMITED -> c.rejectedRateLimited.incrementAndGet();
            case BACKLOG_FULL -> c.rejectedBacklogFull.incrementAndGet();
            case GLOBAL_CAPACITY_EXHAUSTED -> c.rejectedGlobalCapacity.incrementAndGet();
            default -> { }
        }
        return new IngestionException(reason, msg);
    }
}
