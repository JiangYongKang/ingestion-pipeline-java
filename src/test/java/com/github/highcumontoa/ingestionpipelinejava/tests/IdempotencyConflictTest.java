package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordResult;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import com.github.highcumontoa.ingestionpipelinejava.service.SchemaValidator;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.BatchIdempotencyStore;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 场景：重复投递/整批重放幂等；同业务键不同内容标记冲突而非覆盖或丢弃。 */
class IdempotencyConflictTest {

    private static final String SOURCE = "orders";

    private record Harness(IngestionService svc, RecordSink sink, MetricsStore metrics) {
    }

    private Harness harness(Path tmp) {
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp);
        SourceConfig c = new SourceConfig();
        c.setName(SOURCE);
        c.setType("PUSH");
        c.setToken("t");
        c.setBatchSize(100);
        props.setSources(List.of(c));
        SourceRegistry registry = new SourceRegistry(props);
        SchemaStore schemas = new SchemaStore(props);
        SchemaValidator validator = new SchemaValidator(schemas);
        RecordSink sink = new RecordSink(props);
        BatchIdempotencyStore batches = new BatchIdempotencyStore(props);
        DeadLetterStore dlq = new DeadLetterStore(props);
        MetricsStore metrics = new MetricsStore(props);
        RateLimiter rateLimiter = new RateLimiter();
        rateLimiter.register(SOURCE, 1000, 0);
        var governor = new com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor(props);
        var quotas = new com.github.highcumontoa.ingestionpipelinejava.service.QuotaManager(
                registry, rateLimiter, governor);
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rateLimiter, governor, quotas);
        return new Harness(svc, sink, metrics);
    }

    private RecordSchema schemaV1() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true),
                new SchemaField("amount", SchemaField.FieldType.LONG, false)));
    }

    private IncomingRecord rec(String key, long amount) {
        return new IncomingRecord(key, new java.util.LinkedHashMap<>(Map.of("id", key, "amount", amount)));
    }

    @Test
    void duplicateDeliveryAndBatchReplayDoNotDuplicateData(@TempDir Path tmp) {
        String scenario = "duplicate-delivery";
        Harness h = harness(tmp);
        RecordBatch first = new RecordBatch("batch-1", schemaV1(),
                List.of(rec("k1", 100), rec("k2", 200)));
        TestLog.given(scenario, "batch-1: k1=100,k2=200 then replay same batch-1 twice");

        var r1 = h.svc.ingestBatch(SOURCE, first);
        TestLog.actual(scenario, "first:  accepted=" + r1.accepted() + " duplicate=" + r1.duplicate());
        assertEquals(2, r1.accepted());
        assertFalse(r1.replayed());

        var r2 = h.svc.ingestBatch(SOURCE, first);
        TestLog.actual(scenario, "replay: replayed=" + r2.replayed() + " accepted=" + r2.accepted());
        assertTrue(r2.replayed());

        // 不同批次号、相同内容重复投递（重试场景）
        RecordBatch retry = new RecordBatch("batch-1-retry", null,
                List.of(rec("k1", 100), rec("k2", 200)));
        var r3 = h.svc.ingestBatch(SOURCE, retry);
        TestLog.actual(scenario, "retry-batch: accepted=" + r3.accepted() + " duplicate=" + r3.duplicate());
        assertEquals(0, r3.accepted());
        assertEquals(2, r3.duplicate());

        assertEquals(2, h.sink.all(SOURCE).size(), "最终状态只能有 2 条业务记录");
        TestLog.because(scenario, "batchId 去重(整批重放) + 业务键+内容哈希去重(重试) 双重幂等");
    }

    @Test
    void sameKeyDifferentContentIsMarkedConflictAndOriginalKept(@TempDir Path tmp) {
        String scenario = "content-conflict";
        Harness h = harness(tmp);
        h.svc.ingestBatch(SOURCE, new RecordBatch("b1", schemaV1(), List.of(rec("k1", 100))));
        TestLog.given(scenario, "first k1.amount=100; then k1.amount=999");

        var r = h.svc.ingestBatch(SOURCE, new RecordBatch("b2", null, List.of(rec("k1", 999))));
        RecordResult rr = r.records().get(0);
        TestLog.actual(scenario, "status=" + rr.status() + " reason=" + rr.reason() + " detail=" + rr.detail());
        assertEquals(RecordResult.Status.CONFLICT, rr.status());
        assertEquals(com.github.highcumontoa.ingestionpipelinejava.model.RejectReason.RECORD_CONFLICT,
                rr.reason());

        Map<String, Object> finalData = h.sink.all(SOURCE).get("k1").data();
        TestLog.actual(scenario, "stored k1 = " + finalData);
        assertEquals(100L, ((Number) finalData.get("amount")).longValue(),
                "冲突时必须保留首次写入内容，不静默覆盖");
        assertEquals(1, h.sink.all(SOURCE).size(), "冲突不得产生重复行");
        TestLog.because(scenario, "内容哈希不同 -> CONFLICT 并保留原值，差异以 stored/incoming hash 说明");
    }
}
