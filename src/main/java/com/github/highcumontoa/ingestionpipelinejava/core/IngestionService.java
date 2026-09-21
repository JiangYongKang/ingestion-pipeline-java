package com.github.highcumontoa.ingestionpipelinejava.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.error.RecordFailure;
import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.model.WriteOutcome;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRecord;
import com.github.highcumontoa.ingestionpipelinejava.store.DataStore;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetState;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.QuarantineStore;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaState;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 接入处理的核心编排：结构校验 → 幂等写入 → 位点推进，全部在来源锁内完成。
 *
 * <p>位点协议（per-source）：
 * <ul>
 *   <li>正常推进：首批位点必须等于 committed+1，后续严格连续，否则拒绝
 *       （OFFSET_FALLBACK / OFFSET_NON_MONOTONIC）；</li>
 *   <li>整批重放：本批最大位点 ≤ committed 时按幂等重放处理，全部命中
 *       DUPLICATE/CONFLICT 重确认，不改变位点，也不会产生重复数据；</li>
 *   <li>部分重叠（min ≤ committed &lt; max）一律拒绝，防止跳变/错位；</li>
 *   <li>单条坏数据隔离进死信队列，位点仍越过该条，重启后不无限重投。</li>
 * </ul>
 */
public class IngestionService {

    private final OffsetStore offsets;
    private final DataStore data;
    private final SchemaStore schemas;
    private final QuarantineStore quarantine;
    private final MetricsStore metrics;
    private final BacklogGuard backlog;
    private final PipelineProperties props;

    public IngestionService(OffsetStore offsets, DataStore data, SchemaStore schemas,
                            QuarantineStore quarantine, MetricsStore metrics,
                            BacklogGuard backlog, PipelineProperties props) {
        this.offsets = offsets;
        this.data = data;
        this.schemas = schemas;
        this.quarantine = quarantine;
        this.metrics = metrics;
        this.backlog = backlog;
        this.props = props;
    }

    /** 处理一批已通过鉴权与来源解析的记录。 */
    public BatchResult ingest(String sourceId, List<SourceRecord> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            throw new IngestionException(ErrorCode.PARAM_INVALID, "batch must contain at least one record");
        }
        if (incoming.size() > props.getMaxBatchSize()) {
            throw new IngestionException(ErrorCode.BATCH_TOO_LARGE,
                    "batch size " + incoming.size() + " exceeds configured max "
                            + props.getMaxBatchSize());
        }
        SchemaState schemaState = schemas.load(sourceId);
        if (schemaState == null) {
            throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID,
                    "no schema registered for source '" + sourceId
                            + "'; register a schema before sending data");
        }

        List<SourceRecord> sorted = incoming.stream()
                .sorted((a, b) -> Long.compare(a.getOffset(), b.getOffset()))
                .toList();
        long min = sorted.get(0).getOffset();
        long max = sorted.get(sorted.size() - 1).getOffset();
        if (min <= 0) {
            throw new IngestionException(ErrorCode.PARAM_INVALID, "offset must be a positive integer");
        }
        Set<Long> unique = new HashSet<>();
        for (SourceRecord r : sorted) {
            if (!unique.add(r.getOffset())) {
                throw new IngestionException(ErrorCode.OFFSET_NON_MONOTONIC,
                        "duplicate offset " + r.getOffset() + " within batch for '" + sourceId + "'");
            }
        }
        // 批内必须连续，防止 [min, max] 区间内跳过位点造成数据丢失/错位。
        if ((long) sorted.size() != max - min + 1) {
            throw new IngestionException(ErrorCode.OFFSET_NON_MONOTONIC,
                    "non-contiguous offsets within batch for '" + sourceId + "': range ["
                            + min + "," + max + "] expects " + (max - min + 1)
                            + " records but got " + sorted.size());
        }

        backlog.acquire(sourceId, sorted.size());
        try {
            synchronized (offsets.lock(sourceId)) {
                OffsetState state = offsets.load(sourceId);
                long committed = state.getOffset();
                boolean replay = max <= committed;
                if (!replay) {
                    if (min <= committed) {
                        throw new IngestionException(ErrorCode.OFFSET_FALLBACK,
                                "partially overlapping batch refused for '" + sourceId
                                        + "': committed=" + committed + ", batch=[" + min + "," + max
                                        + "]; resend the whole historical batch for replay, or start at "
                                        + (committed + 1));
                    }
                    if (min != committed + 1) {
                        throw new IngestionException(ErrorCode.OFFSET_NON_MONOTONIC,
                                "non-contiguous batch refused for '" + sourceId + "': committed="
                                        + committed + ", first offset=" + min + ", expected "
                                        + (committed + 1));
                    }
                }
                BatchResult result = new BatchResult();
                result.setSourceId(sourceId);
                result.setFromOffset(committed);
                for (SourceRecord rec : sorted) {
                    processOne(sourceId, schemaState, rec, result);
                }
                if (!replay) {
                    OffsetState advanced = offsets.advance(sourceId, state.getVersion(), max);
                    result.setCommittedOffset(advanced.getOffset());
                    data.compact(sourceId, advanced.getOffset());
                } else {
                    result.setCommittedOffset(committed);
                }
                applyMetrics(sourceId, result);
                return result;
            }
        } finally {
            backlog.release(sourceId, sorted.size());
        }
    }

    private void processOne(String sourceId, SchemaState schemaState, SourceRecord rec, BatchResult result) {
        result.setReceived(result.getReceived() + 1);
        String rawJson = rec.getRawJson();
        JsonNode raw;
        try {
            raw = JsonFiles.mapper().readTree(rawJson);
        } catch (Exception e) {
            quarantine(sourceId, null, rec.getOffset(), rawJson,
                    ErrorCode.DATA_INVALID, "invalid JSON: " + e.getMessage(), result);
            return;
        }
        JsonNode normalized;
        String key;
        try {
            normalized = SchemaValidator.normalize(schemaState, raw);
            key = SchemaValidator.businessKey(schemaState.getCurrent(), normalized);
        } catch (IngestionException e) {
            String guessedKey = guessKey(schemaState, raw);
            quarantine(sourceId, guessedKey, rec.getOffset(), rawJson, e.getCode(), e.getMessage(), result);
            return;
        }
        WriteOutcome outcome = data.write(sourceId, key, normalized, rec.getOffset());
        switch (outcome) {
            case INSERTED -> result.setInserted(result.getInserted() + 1);
            case DUPLICATE -> result.setDuplicates(result.getDuplicates() + 1);
            case CONFLICT -> {
                result.setConflicts(result.getConflicts() + 1);
                RecordFailure f = new RecordFailure();
                f.setSourceId(sourceId);
                f.setOffset(rec.getOffset());
                f.setBusinessKey(key);
                f.setCode(ErrorCode.CONFLICT);
                f.setReason("same business key with different content; existing record kept, "
                        + "incoming version stored as conflict evidence");
                f.setRawJson(rawJson);
                f.setAttempts(1);
                f.setFirstFailedAt(System.currentTimeMillis());
                f.setLastFailedAt(f.getFirstFailedAt());
                result.getFailures().add(f);
            }
        }
    }

    private void quarantine(String sourceId, String key, long offset, String rawJson,
                            ErrorCode code, String reason, BatchResult result) {
        RecordFailure f = quarantine.recordFailure(sourceId, key, offset, rawJson, code, reason,
                props.getMaxRetryAttempts());
        result.setQuarantined(result.getQuarantined() + 1);
        result.getFailures().add(f);
    }

    /**
     * 按策略重试隔离记录：重新执行校验与幂等写入。成功则移出隔离区；
     * 失败累加 attempts，达到上限后标记 exhausted，不再自动重投。位点不变。
     */
    public BatchResult retryOne(String sourceId, String businessKey, String fixedRawJson) {
        SchemaState schemaState = schemas.load(sourceId);
        if (schemaState == null) {
            throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID,
                    "no schema registered for source '" + sourceId + "'");
        }
        synchronized (offsets.lock(sourceId)) {
            RecordFailure target = quarantine.list(sourceId).stream()
                    .filter(f -> businessKey.equals(f.getBusinessKey()))
                    .findFirst()
                    .orElseThrow(() -> new IngestionException(ErrorCode.PARAM_INVALID,
                            "no quarantined record with key '" + businessKey
                                    + "' for source '" + sourceId + "'"));
            if (target.isExhausted()) {
                throw new IngestionException(ErrorCode.PARAM_INVALID,
                        "quarantined record '" + businessKey + "' is exhausted after "
                                + target.getAttempts() + " attempts");
            }
            String rawJson = fixedRawJson != null && !fixedRawJson.isBlank()
                    ? fixedRawJson : target.getRawJson();
            if (fixedRawJson != null && !fixedRawJson.isBlank()) {
                quarantine.updateRaw(sourceId, businessKey, rawJson);
            }
            BatchResult result = new BatchResult();
            result.setSourceId(sourceId);
            result.setFromOffset(offsets.load(sourceId).getOffset());
            SourceRecord rec = new SourceRecord(target.getOffset(), rawJson);
            processOne(sourceId, schemaState, rec, result);
            boolean recovered = result.getFailures().isEmpty();
            if (recovered) {
                quarantine.remove(sourceId, businessKey);
            }
            metrics.increment(sourceId, m -> {
                m.setRetried(m.getRetried() + 1);
                m.setReceived(m.getReceived() + result.getReceived());
                m.setInserted(m.getInserted() + result.getInserted());
                m.setDuplicates(m.getDuplicates() + result.getDuplicates());
                m.setConflicts(m.getConflicts() + result.getConflicts());
                if (!recovered) {
                    m.setQuarantined(m.getQuarantined() + 1);
                }
            });
            result.setCommittedOffset(offsets.load(sourceId).getOffset());
            return result;
        }
    }

    private void applyMetrics(String sourceId, BatchResult r) {
        metrics.increment(sourceId, m -> {
            m.setReceived(m.getReceived() + r.getReceived());
            m.setInserted(m.getInserted() + r.getInserted());
            m.setDuplicates(m.getDuplicates() + r.getDuplicates());
            m.setConflicts(m.getConflicts() + r.getConflicts());
            m.setQuarantined(m.getQuarantined() + r.getQuarantined());
        });
    }

    private static String guessKey(SchemaState state, JsonNode raw) {
        if (raw != null && raw.isObject()) {
            JsonNode k = raw.get(state.getCurrent().getKeyField());
            if (k != null && !k.isNull()) {
                return k.isTextual() ? k.asText() : k.toString();
            }
        }
        return null;
    }
}
