package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import com.github.highcumontoa.ingestionpipelinejava.service.SchemaValidator;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.service.pull.FileSourceReader;
import com.github.highcumontoa.ingestionpipelinejava.service.pull.PullCoordinator;
import com.github.highcumontoa.ingestionpipelinejava.service.pull.SourceReader;
import com.github.highcumontoa.ingestionpipelinejava.store.BatchIdempotencyStore;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 场景：文件拉取按位点连续推进；坏行进 DLQ 不中断整批；中断重启从已确认位点继续。 */
class PullFileSourceTest {

    private static final String SOURCE = "inv";

    private record Ctx(PullCoordinator coordinator, OffsetStore offsets, RecordSink sink,
                       DeadLetterStore dlq, Path fileDir) {
    }

    private Ctx build(Path tmp) {
        Path fileDir = tmp.resolve("filedir");
        try {
            Files.createDirectories(fileDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp.resolve("state"));
        SourceConfig c = new SourceConfig();
        c.setName(SOURCE);
        c.setType("PULL_FILE");
        c.setToken("t");
        c.setDir(fileDir.toString());
        c.setBatchSize(100);
        c.setPollIntervalMs(60_000L);
        props.setSources(List.of(c));
        SourceRegistry registry = new SourceRegistry(props);
        SchemaStore schemas = new SchemaStore(props);
        // 预置 v1（模拟重启：磁盘上已存在结构时不重复声明）
        if (!schemas.exists(SOURCE)) {
            schemas.register(SOURCE, new com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema(1,
                    List.of(new com.github.highcumontoa.ingestionpipelinejava.model.SchemaField(
                            "sku", com.github.highcumontoa.ingestionpipelinejava.model.SchemaField.FieldType.STRING, true))));
        }
        SchemaValidator validator = new SchemaValidator(schemas);
        RecordSink sink = new RecordSink(props);
        var batches = new BatchIdempotencyStore(props);
        DeadLetterStore dlq = new DeadLetterStore(props);
        OffsetStore offsets = new OffsetStore(props);
        MetricsStore metrics = new MetricsStore(props);
        RateLimiter rl = new RateLimiter();
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl);
        PullCoordinator coord = new PullCoordinator(registry, svc, offsets, dlq, rl, metrics, false);
        return new Ctx(coord, offsets, sink, dlq, fileDir);
    }

    private void writeLines(Path dir, String... lines) throws Exception {
        Path f = dir.resolve("records-001.jsonl");
        Files.write(f, String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    }

    private String line(String sku) {
        return "{\"key\":\"" + sku + "\",\"data\":{\"sku\":\"" + sku + "\"}}";
    }

    @Test
    void pullsWithOffsetResumeAndIsolatesMalformedLines(@TempDir Path tmp) throws Exception {
        String scenario = "pull-file-resume";
        Ctx ctx = build(tmp);
        writeLines(ctx.fileDir(), line("s1"), "{not-json", line("s2"), line("s3"));
        TestLog.given(scenario, "jsonl lines: s1, <malformed>, s2, s3");

        int n1 = ctx.coordinator().tick(SOURCE);
        TestLog.actual(scenario, "tick#1 processed=" + n1 + " offset=" + ctx.offsets().current(SOURCE));
        assertEquals(4, n1, "坏行也占用一个位点，保证连续");
        assertEquals(4, ctx.offsets().current(SOURCE));
        assertEquals(3, ctx.sink().all(SOURCE).size(), "3 条好数据全部落盘");
        assertEquals(1, ctx.dlq().all(SOURCE).size(), "1 条坏行进入 DLQ");
        assertEquals(RejectReason.MALFORMED_BODY, ctx.dlq().all(SOURCE).get(0).getReason());
        TestLog.actual(scenario, "DLQ[0] reason=" + ctx.dlq().all(SOURCE).get(0).getReason()
                + " detail=" + ctx.dlq().all(SOURCE).get(0).getErrorDetail());

        // 模拟重启：新建协调器与各存储实例（从磁盘恢复），再 tick 不应重复处理
        Ctx restarted = build(tmp);
        int n2 = restarted.coordinator().tick(SOURCE);
        TestLog.actual(scenario, "tick#2 after restart processed=" + n2
                + " offset=" + restarted.offsets().current(SOURCE));
        assertEquals(0, n2, "无新增数据时不得重放旧数据");
        assertEquals(4, restarted.offsets().current(SOURCE), "位点从磁盘恢复为 4");
        assertEquals(3, restarted.sink().all(SOURCE).size());
        TestLog.because(scenario, "位点即已确认位置；重启后 fromExclusive=4，只读取新增行");

        // 追加新行后再 tick，从第 5 行继续
        Files.writeString(ctx.fileDir.resolve("records-001.jsonl"),
                "\n" + line("s4"), StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.APPEND);
        int n3 = restarted.coordinator().tick(SOURCE);
        TestLog.actual(scenario, "tick#3 after append processed=" + n3);
        assertEquals(1, n3);
        assertEquals(5, restarted.offsets().current(SOURCE));
        assertTrue(restarted.sink().all(SOURCE).containsKey("s4"));
    }

    @Test
    void readerReturnsContiguousSequencesAndHonoursBatchBound(@TempDir Path tmp) throws Exception {
        String scenario = "reader-bounded";
        Ctx ctx = build(tmp);
        writeLines(ctx.fileDir(), line("a"), line("b"), line("c"));
        FileSourceReader reader = new FileSourceReader(ctx.fileDir());
        List<SourceReader.PulledRecord> page1 = reader.read(0, 2);
        TestLog.given(scenario, "3 lines, batchSize=2 then fromExclusive=2");
        assertEquals(List.of(1L, 2L), page1.stream().map(SourceReader.PulledRecord::sequence).toList());
        List<SourceReader.PulledRecord> page2 = reader.read(2, 2);
        assertEquals(List.of(3L), page2.stream().map(SourceReader.PulledRecord::sequence).toList());
        TestLog.actual(scenario, "page1=" + List.of(1, 2) + " page2=" + List.of(3));
        TestLog.because(scenario, "分页有界读取：内存中最多保留一个批次，不整文件加载");
    }

    @Test
    void sourceFreezesOnSequenceGapAndOffsetStaysPut(@TempDir Path tmp) throws Exception {
        String scenario = "pull-gap-freeze";
        Ctx ctx = build(tmp);
        writeLines(ctx.fileDir(), line("s1"));
        ctx.coordinator().tick(SOURCE);
        assertEquals(1, ctx.offsets().current(SOURCE));

        // 注入一个“从位点 2 开始却返回 seq=3”的读取器，模拟源端缺口
        ctx.coordinator().registerReader(SOURCE, (fromExclusive, maxRecords) -> List.of(
                new SourceReader.PulledRecord(3L,
                        new com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord(
                                "x", java.util.Map.of("sku", "x")), null)));
        TestLog.given(scenario, "committed offset=1; reader returns sequence=3 (2 missing)");

        IngestionException ex = assertThrows(IngestionException.class,
                () -> ctx.coordinator().tick(SOURCE));
        TestLog.actual(scenario, ex.reason() + " | " + ex.getMessage());
        assertEquals(RejectReason.OFFSET_FROZEN, ex.reason());
        assertEquals(1, ctx.offsets().current(SOURCE), "冻结后位点必须停在 1，不越位");
        assertTrue(ctx.coordinator().isFrozen(SOURCE));
        TestLog.because(scenario, "缺口 => 冻结来源并保持已确认位点，绝不跳过缺失数据继续写");
    }
}
