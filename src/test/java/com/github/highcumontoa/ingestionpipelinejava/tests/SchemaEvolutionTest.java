package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.DeadLetter;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordResult;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import com.github.highcumontoa.ingestionpipelinejava.service.SchemaValidator;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.BatchIdempotencyStore;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 场景：新增字段默认 null 且稳定；缺失/类型不符/删列/未声明列被隔离并说明原因。 */
class SchemaEvolutionTest {

    private static final String SOURCE = "evol";

    private record Harness(IngestionService svc, SchemaValidator validator, SchemaStore schemas,
                           RecordSink sink, DeadLetterStore dlq) {
    }

    private Harness harness(Path tmp) {
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp);
        var c = new com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig();
        c.setName(SOURCE);
        c.setType("PUSH");
        c.setToken("t");
        c.setBatchSize(100);
        props.setSources(List.of(c));
        SourceRegistry registry = new SourceRegistry(props);
        SchemaStore schemas = new SchemaStore(props);
        SchemaValidator validator = new SchemaValidator(schemas);
        RecordSink sink = new RecordSink(props);
        var batches = new BatchIdempotencyStore(props);
        DeadLetterStore dlq = new DeadLetterStore(props);
        MetricsStore metrics = new MetricsStore(props);
        RateLimiter rl = new RateLimiter();
        rl.register(SOURCE, 1000, 0);
        var governor = new com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor(props);
        var quotas = new com.github.highcumontoa.ingestionpipelinejava.service.QuotaManager(registry, rl, governor);
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl, governor, quotas);
        return new Harness(svc, validator, schemas, sink, dlq);
    }

    private RecordSchema v1() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true),
                new SchemaField("age", SchemaField.FieldType.LONG, true)));
    }

    private IncomingRecord record(Map<String, Object> data) {
        return new IncomingRecord((String) data.get("id"), new LinkedHashMap<>(data));
    }

    @Test
    void addedOptionalFieldDefaultsToNullAndBadRowsAreQuarantined(@TempDir Path tmp) {
        String scenario = "schema-evolution";
        Harness h = harness(tmp);
        // v1 批次：一条好数据 + 一条缺必填 age（坏数据不得中断整批）
        RecordBatch b1 = new RecordBatch("b1", v1(), List.of(
                record(Map.of("id", "r1", "age", 30L)),
                record(Map.of("id", "r2"))));
        var r1 = h.svc.ingestBatch(SOURCE, b1);
        TestLog.given(scenario, "v1 batch: r1 ok, r2 missing required 'age'");
        TestLog.actual(scenario, "accepted=" + r1.accepted() + " quarantined=" + r1.quarantined()
                + " reason=" + r1.records().get(1).reason());
        assertEquals(1, r1.accepted());
        assertEquals(RecordResult.Status.QUARANTINED, r1.records().get(1).status());
        assertEquals(RejectReason.MISSING_FIELD, r1.records().get(1).reason());
        assertEquals(1, h.sink.all(SOURCE).size(), "坏数据不得污染已有状态");

        // 演进到 v2：新增可选字段 nickname，删除 age
        RecordSchema v2 = new RecordSchema(2, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true),
                new SchemaField("nickname", SchemaField.FieldType.STRING, false)));
        int version = h.schemas.register(SOURCE, v2).version();
        TestLog.actual(scenario, "declared schema version=" + version);

        // 旧格式数据在 v2 下：nickname 缺省 -> null（确定且稳定）
        var r2 = h.svc.ingestBatch(SOURCE, new RecordBatch("b2", null,
                List.of(record(Map.of("id", "r3")))));
        TestLog.actual(scenario, "old-shape row accepted=" + r2.accepted());
        assertEquals(1, r2.accepted());
        Map<String, Object> stored = h.sink.all(SOURCE).get("r3").data();
        assertNull(stored.get("nickname"), "新增字段默认语义必须为 null");
        TestLog.because(scenario, "新增字段只能可选，缺失即稳定默认 null，不补魔术值");

        // 出现已删除列 age -> COLUMN_DROPPED
        var r3 = h.svc.ingestBatch(SOURCE, new RecordBatch("b3", null,
                List.of(record(Map.of("id", "r4", "age", 9L)))));
        assertEquals(RejectReason.COLUMN_DROPPED, r3.records().get(0).reason());
        TestLog.actual(scenario, "dropped-column row -> " + r3.records().get(0).reason());

        // 出现未声明列 -> UNKNOWN_FIELD
        var r4 = h.svc.ingestBatch(SOURCE, new RecordBatch("b4", null,
                List.of(record(Map.of("id", "r5", "mystery", 1)))));
        assertEquals(RejectReason.UNKNOWN_FIELD, r4.records().get(0).reason());
        TestLog.actual(scenario, "unknown-field row -> " + r4.records().get(0).reason());

        // 类型不匹配 -> TYPE_MISMATCH（id 给了数字）
        var r5 = h.svc.ingestBatch(SOURCE, new RecordBatch("b5", null,
                List.of(new IncomingRecord("r6", new LinkedHashMap<>(Map.of("id", 123L))))));
        assertEquals(RejectReason.TYPE_MISMATCH, r5.records().get(0).reason());
        TestLog.actual(scenario, "type-mismatch row -> " + r5.records().get(0).reason()
                + " " + r5.records().get(0).detail());

        List<DeadLetter> dlqAll = h.dlq.all(SOURCE);
        TestLog.actual(scenario, "DLQ size=" + dlqAll.size() + " reasons="
                + dlqAll.stream().map(DeadLetter::getReason).toList());
        assertEquals(4, dlqAll.size());
        TestLog.because(scenario, "四类坏数据各入 DLQ 并保留原始数据+归类原因，已写入数据不受影响");
    }

    @Test
    void schemaVersionMustAdvanceByOneAndNewFieldsMustBeOptional(@TempDir Path tmp) {
        String scenario = "schema-version-rules";
        Harness h = harness(tmp);
        h.schemas.register(SOURCE, v1());

        RecordSchema jump = new RecordSchema(3, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true)));
        IngestionException e1 = assertThrows(IngestionException.class,
                () -> h.schemas.register(SOURCE, jump));
        TestLog.actual(scenario, "1->3 => " + e1.reason());
        assertEquals(RejectReason.SCHEMA_VERSION_INVALID, e1.reason());

        RecordSchema requiredNew = new RecordSchema(2, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true),
                new SchemaField("ssn", SchemaField.FieldType.STRING, true)));
        IngestionException e2 = assertThrows(IngestionException.class,
                () -> h.schemas.register(SOURCE, requiredNew));
        TestLog.actual(scenario, "new REQUIRED field => " + e2.reason() + " | " + e2.getMessage());
        assertEquals(RejectReason.SCHEMA_VERSION_INVALID, e2.reason());
        TestLog.because(scenario, "版本只能 +1；新增必填字段无法确定默认值，故拒绝");
    }
}
