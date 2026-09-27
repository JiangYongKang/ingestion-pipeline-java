package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.MetricSnapshot;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.QuotaService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import com.github.highcumontoa.ingestionpipelinejava.service.SchemaValidator;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.service.pull.PullCoordinator;
import com.github.highcumontoa.ingestionpipelinejava.store.BatchIdempotencyStore;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 场景：进程级总容量治理。
 * 同一进程内推送+拉取两类来源共享同一份总容量；配额热调整立即生效；
 * 容量拒绝零写入零位点变更、计数可区分且与实际被拦次数一致。
 */
class GlobalCapacityGovernanceTest {

    private static final String PUSH = "push-src";
    private static final String PULL = "pull-src";

    private record Harness(IngestionService svc, PullCoordinator pull, RecordSink sink,
                           OffsetStore offsets, MetricsStore metrics, RateLimiter rl,
                           QuotaService quotas, CapacityGovernor governor, Path fileDir) {
    }

    /** 同一进程装配一个推送源 + 一个文件拉取源，共享总量为 total 的全局容量池。 */
    private Harness harness(Path tmp, int total, int pushRate, int pullBatchSize) throws Exception {
        Path fileDir = tmp.resolve("filedir");
        Files.createDirectories(fileDir);
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp.resolve("state"));
        SourceConfig push = new SourceConfig();
        push.setName(PUSH);
        push.setType("PUSH");
        push.setToken("t1");
        push.setBatchSize(100);
        push.setRateLimitPerSec(pushRate);
        push.setMaxInflight(1000);
        SourceConfig pull = new SourceConfig();
        pull.setName(PULL);
        pull.setType("PULL_FILE");
        pull.setToken("t2");
        pull.setDir(fileDir.toString());
        pull.setBatchSize(pullBatchSize);
        pull.setPollIntervalMs(60_000L);
        props.setSources(List.of(push, pull));

        SourceRegistry registry = new SourceRegistry(props);
        SchemaStore schemas = new SchemaStore(props);
        SchemaValidator validator = new SchemaValidator(schemas);
        RecordSink sink = new RecordSink(props);
        var batches = new BatchIdempotencyStore(props);
        DeadLetterStore dlq = new DeadLetterStore(props);
        OffsetStore offsets = new OffsetStore(props);
        MetricsStore metrics = new MetricsStore(props);
        RateLimiter rl = new RateLimiter();
        rl.register(PUSH, 1000, pushRate);
        rl.register(PULL, 1000, 0);
        QuotaService quotas = new QuotaService(registry, rl);
        CapacityGovernor governor = new CapacityGovernor(total, 2);
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl, quotas, governor);
        PullCoordinator coord = new PullCoordinator(registry, svc, offsets, dlq, rl, metrics,
                quotas, governor, false);
        // 拉取源预置结构（等价于管理面声明）
        schemas.register(PULL, new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true))));
        return new Harness(svc, coord, sink, offsets, metrics, rl, quotas, governor, fileDir);
    }

    private RecordSchema pushSchema() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true)));
    }

    private IncomingRecord rec(String key) {
        return new IncomingRecord(key, new LinkedHashMap<>(Map.of("id", key)));
    }

    private void writePullLines(Path dir, int from, int to) throws Exception {
        List<String> lines = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            lines.add("{\"key\":\"p" + i + "\",\"data\":{\"id\":\"p" + i + "\"}}");
        }
        Files.write(dir.resolve("records-001.jsonl"),
                String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    }

    private MetricSnapshot snap(Harness h, String source) {
        return h.metrics.inMemorySnapshot(source, h.offsets.current(source), h.rl.inflight(source));
    }

    @Test
    void pushAndPullShareTheSameGlobalCapacityPool(@TempDir Path tmp) throws Exception {
        String scenario = "shared-global-capacity";
        // total=1：全局池只有 1 个许可，两类来源竞争同一份容量
        Harness h = harness(tmp, 1, 0, 10);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        writePullLines(h.fileDir, 1, 5);
        TestLog.given(scenario, "total=1, push holds the only permit; pull tick arrives");

        // 模拟一个在途推送批次占住唯一许可
        assertTrue(h.governor.tryAcquire(PUSH));

        // 拉取侧：被同一份总容量拦下，零写入、零位点推进，计数+1
        int processed = h.pull.tick(PULL);
        MetricSnapshot pullSnap = snap(h, PULL);
        TestLog.actual(scenario + "-pull", "processed=" + processed
                + " offset=" + pullSnap.offset()
                + " rejectedGlobalCapacity=" + pullSnap.rejectedGlobalCapacity());
        assertEquals(0, processed, "总容量到顶，拉取本轮必须被拦");
        assertEquals(0, pullSnap.offset(), "被拦不得推进位点");
        assertEquals(0, h.sink.all(PULL).size(), "被拦不得产生任何写入");
        assertEquals(1, pullSnap.rejectedGlobalCapacity(), "计数与实际被拦次数一致");
        assertEquals(1, pullSnap.rejected());

        // 推送侧：同样被拦（自身份额 1 已被在途批次占用）
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("a")))));
        TestLog.actual(scenario + "-push", ex.reason().toString());
        assertEquals(RejectReason.GLOBAL_CAPACITY_EXHAUSTED, ex.reason());
        assertEquals(0, h.sink.all(PUSH).size(), "推送被拦同样零写入");
        assertEquals(1, snap(h, PUSH).rejectedGlobalCapacity());

        // 释放后两类来源都恢复：不丢数据
        h.governor.release(PUSH);
        assertEquals(5, h.pull.tick(PULL), "容量释放后拉取继续，源端数据不丢");
        BatchResult ok = h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("a"))));
        assertEquals(1, ok.accepted(), "同一批次重试成功，此前拒绝未造成部分写入");
        TestLog.because(scenario, "推送与拉取从同一全局池取许可；到顶即 GLOBAL_CAPACITY_EXHAUSTED，"
                + "释放后双方恢复，数据不丢不重复");
    }

    @Test
    void fairSharePreventsStarvation(@TempDir Path tmp) throws Exception {
        String scenario = "fair-share";
        // total=2, N=2 -> 每来源公平份额=1；推送占满自身份额也挤不占拉取的份额
        Harness h = harness(tmp, 2, 0, 10);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        writePullLines(h.fileDir, 1, 3);
        TestLog.given(scenario, "total=2, share=1 per source; push occupies its whole share");

        assertTrue(h.governor.tryAcquire(PUSH)); // 推送占满自己的份额
        int processed = h.pull.tick(PULL);
        TestLog.actual(scenario, "pull processed=" + processed
                + " while push holds its share; snapshot=" + h.governor.snapshot());
        assertEquals(3, processed, "拉取的公平份额不受推送占用影响（确定、可复现）");
        // 推送超出自身份额再取 -> 被拒
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("a")))));
        assertEquals(RejectReason.GLOBAL_CAPACITY_EXHAUSTED, ex.reason());
        TestLog.because(scenario, "share=max(1,total/N)，份额之和<=总量，"
                + "任何来源长期占满自身份额也无法让其它来源拿不到配额");
    }

    @Test
    void hotQuotaUpdateAppliesImmediatelyAndRejectsInvalid(@TempDir Path tmp) throws Exception {
        String scenario = "hot-quota-update";
        Harness h = harness(tmp, 100, 1, 1); // 推送限速 1/s，拉取单批 1
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        TestLog.given(scenario, "push rateLimit=1/s, pull batchSize=1; update at runtime");

        // 1) 限速热调整：先耗尽令牌确认被限，再调大立即放行
        h.svc.ingestBatch(PUSH, new RecordBatch("r1", null, List.of(rec("a"))));
        IngestionException limited = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("r2", null, List.of(rec("b")))));
        assertEquals(RejectReason.RATE_LIMITED, limited.reason());
        var q = h.quotas.update(PUSH, null, 1000, null);
        TestLog.actual(scenario + "-rate", "new quota=" + q + "; retry r2 immediately");
        assertEquals(1, h.svc.ingestBatch(PUSH, new RecordBatch("r2", null, List.of(rec("b")))).accepted(),
                "热调整后的首批请求立即按新配额放行");

        // 2) 单批规模热调整：调小即拒、调回即通
        h.quotas.update(PUSH, 1, null, null);
        IngestionException tooLarge = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("r3", null,
                        List.of(rec("c"), rec("d")))));
        assertEquals(RejectReason.BATCH_TOO_LARGE, tooLarge.reason());
        h.quotas.update(PUSH, 10, null, null);
        assertEquals(2, h.svc.ingestBatch(PUSH, new RecordBatch("r3", null,
                List.of(rec("c"), rec("d")))).accepted());

        // 3) 拉取单批规模热调整：1 -> 3，下一轮立即多读
        writePullLines(h.fileDir, 1, 4);
        assertEquals(1, h.pull.tick(PULL), "调整前每轮 1 条");
        h.quotas.update(PULL, 3, null, null);
        assertEquals(3, h.pull.tick(PULL), "调整后下一轮立即按新批量拉取");
        assertEquals(4, h.offsets.current(PULL), "位点连续推进不错乱");

        // 4) 非法配额整体拒绝，生效配置不变
        var before = h.quotas.effective(PUSH);
        for (var bad : List.of(
                new Object[]{0, null, null}, new Object[]{null, -1, null}, new Object[]{null, null, 0})) {
            IngestionException invalid = assertThrows(IngestionException.class, () ->
                    h.quotas.update(PUSH, (Integer) bad[0], (Integer) bad[1], (Integer) bad[2]));
            assertEquals(RejectReason.INVALID_QUOTA, invalid.reason());
        }
        assertEquals(before, h.quotas.effective(PUSH), "非法调整后生效配置保持不变");
        TestLog.because(scenario, "配额就地热更新（不重置在途/令牌），只对此后处理生效；"
                + "非法值 INVALID_QUOTA(400) 且当前配置不变");
    }

    @Test
    void replayDoesNotInflateRejectionCounters(@TempDir Path tmp) throws Exception {
        String scenario = "replay-vs-counters";
        Harness h = harness(tmp, 100, 1, 10);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        TestLog.given(scenario, "rate=1/s; one rejected batch redelivered, one ok batch replayed");

        // 被拒批次：每次实际被拦计 1 次
        h.svc.ingestBatch(PUSH, new RecordBatch("ok", null, List.of(rec("a"))));
        for (int i = 0; i < 3; i++) {
            IngestionException ex = assertThrows(IngestionException.class,
                    () -> h.svc.ingestBatch(PUSH, new RecordBatch("blocked", null, List.of(rec("x")))));
            assertEquals(RejectReason.RATE_LIMITED, ex.reason());
        }
        MetricSnapshot s1 = snap(h, PUSH);
        TestLog.actual(scenario, "rejectedRateLimited=" + s1.rejectedRateLimited()
                + " rejected=" + s1.rejected());
        assertEquals(3, s1.rejectedRateLimited(), "3 次实际被拦 = 计数 3，不多不少");
        assertEquals(3, s1.rejected());

        // 成功批次的整批重放：返回首次结果，不产生任何拒绝计数或重复写入
        BatchResult replay = h.svc.ingestBatch(PUSH, new RecordBatch("ok", null, List.of(rec("a"))));
        assertTrue(replay.replayed());
        MetricSnapshot s2 = snap(h, PUSH);
        assertEquals(3, s2.rejectedRateLimited(), "重放不得累加拒绝计数");
        assertEquals(3, s2.rejected());
        assertEquals(1, h.sink.all(PUSH).size(), "重放不产生重复写入");
        TestLog.because(scenario, "批次幂等检查在容量扣减之前：重放直接返回首次结果，统计不失真");
    }
}
