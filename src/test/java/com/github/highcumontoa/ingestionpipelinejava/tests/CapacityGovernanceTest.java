package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import com.github.highcumontoa.ingestionpipelinejava.service.CapacityGovernor;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.QuotaManager;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 场景：进程级容量治理 —— 同一进程内推送 + 拉取两类来源共用一份总容量；
 * 配额热调整即时生效且不影响已接收数据；拒绝按来源/原因可解释且计数不失真。
 */
class CapacityGovernanceTest {

    private static final String PUSH = "push-src";
    private static final String PULL = "pull-src";

    private record Harness(IngestionService svc, PullCoordinator coord, RecordSink sink,
                           CapacityGovernor governor, QuotaManager quotas,
                           MetricsStore metrics, OffsetStore offsets, Path fileDir) {
    }

    /** 同一进程装配一个推送来源 + 一个文件拉取来源，共享 globalMax 的总容量。 */
    private Harness harness(Path tmp, int globalMax) throws Exception {
        Path fileDir = tmp.resolve("files");
        Files.createDirectories(fileDir);
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp.resolve("state"));
        props.setGlobalMaxInflight(globalMax);

        SourceConfig push = new SourceConfig();
        push.setName(PUSH);
        push.setType("PUSH");
        push.setToken("tp");
        push.setBatchSize(100);

        SourceConfig pull = new SourceConfig();
        pull.setName(PULL);
        pull.setType("PULL_FILE");
        pull.setToken("tf");
        pull.setDir(fileDir.toString());
        pull.setBatchSize(100);
        pull.setPollIntervalMs(60_000L);
        props.setSources(List.of(push, pull));

        SourceRegistry registry = new SourceRegistry(props);
        SchemaStore schemas = new SchemaStore(props);
        schemas.register(PULL, schema());
        SchemaValidator validator = new SchemaValidator(schemas);
        RecordSink sink = new RecordSink(props);
        var batches = new BatchIdempotencyStore(props);
        DeadLetterStore dlq = new DeadLetterStore(props);
        OffsetStore offsets = new OffsetStore(props);
        MetricsStore metrics = new MetricsStore(props);
        RateLimiter rl = new RateLimiter();
        CapacityGovernor governor = new CapacityGovernor(props);
        QuotaManager quotas = new QuotaManager(registry, rl, governor);
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl, governor, quotas);
        PullCoordinator coord = new PullCoordinator(registry, svc, offsets, dlq, rl,
                governor, quotas, metrics, false);
        return new Harness(svc, coord, sink, governor, quotas, metrics, offsets, fileDir);
    }

    private static RecordSchema schema() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true)));
    }

    private static IncomingRecord rec(String key) {
        return new IncomingRecord(key, new LinkedHashMap<>(Map.of("id", key)));
    }

    private void writePullLines(Path dir, String... keys) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            sb.append("{\"key\":\"").append(k).append("\",\"data\":{\"id\":\"").append(k).append("\"}}\n");
        }
        Files.write(dir.resolve("records-001.jsonl"), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void pushAndPullShareOneGlobalCapacityWithDeterministicFairness(@TempDir Path tmp) throws Exception {
        String scenario = "global-capacity-shared";
        // 总容量 2、两个来源 -> 每来源确定份额 1
        Harness h = harness(tmp, 2);
        writePullLines(h.fileDir, "p1", "p2");
        TestLog.given(scenario, "globalMaxInflight=2, sources=push+pull -> fairShare=1 each");

        assertEquals(2, h.governor.totalPermits());
        assertEquals(1, h.governor.fairShare());

        // 先声明结构（此时容量空闲），再制造推送侧占满份额的压力
        h.svc.ingestBatch(PUSH, new RecordBatch("init", schema(), List.of()));

        // 推送侧占满自己的份额（模拟一个在途批次）
        assertTrue(h.governor.tryAcquire(PUSH));
        // 确定性公平：即使全局还有剩余，推送侧也不能侵占拉取侧的份额
        assertFalse(h.governor.tryAcquire(PUSH), "超过公平份额必须失败，行为可复现");

        // 拉取侧长期拿不到配额？不会：自己的份额始终可用，正常推进
        int processed = h.coord.tick(PULL);
        TestLog.actual(scenario, "pull tick processed=" + processed
                + " while push holds its share; offset=" + h.offsets.current(PULL));
        assertEquals(2, processed, "推送侧占满自身份额时，拉取侧仍能拿到自己的份额");
        assertEquals(2, h.offsets.current(PULL));
        assertEquals(2, h.sink.all(PULL).size());

        // 推送侧份额被占期间，新推送批次被总容量明确拒绝，零写入
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("a")))));
        TestLog.actual(scenario + "-push", ex.reason() + " | " + ex.getMessage());
        assertEquals(RejectReason.GLOBAL_CAPACITY_EXCEEDED, ex.reason());
        assertEquals(0, h.sink.all(PUSH).size(), "总容量拒绝不得产生任何写入");
        assertEquals(1, h.metrics.forSource(PUSH).rejectedGlobal.get());
        assertEquals(0, h.metrics.forSource(PUSH).rejectedBacklog.get());

        // 释放后同批次可正常处理：不丢数据
        h.governor.release(PUSH);
        var r = h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("a"))));
        assertEquals(1, r.accepted());
        assertEquals(1, h.sink.all(PUSH).size());
        TestLog.because(scenario, "所有来源共用同一份总许可；每来源份额=floor(总/来源数)，"
                + "超额即 GLOBAL_CAPACITY_EXCEEDED，份额预留保证任何来源不被饿死");
    }

    @Test
    void pullTickRejectedByGlobalCapacityLeavesOffsetAndDataUntouched(@TempDir Path tmp) throws Exception {
        String scenario = "pull-global-reject";
        Harness h = harness(tmp, 2);
        writePullLines(h.fileDir, "p1", "p2", "p3");
        TestLog.given(scenario, "pull share held externally (in-flight), then tick");

        // 拉取侧唯一份额被一个在途轮次占用（模拟上一轮尚未结束的全局许可）
        assertTrue(h.governor.tryAcquire(PULL));
        int processed = h.coord.tick(PULL);
        TestLog.actual(scenario, "processed=" + processed + " offset=" + h.offsets.current(PULL)
                + " rejectedGlobal=" + h.metrics.forSource(PULL).rejectedGlobal.get());
        assertEquals(0, processed, "总容量到顶：本轮明确放弃，不排队");
        assertEquals(0, h.offsets.current(PULL), "位点不得变化");
        assertEquals(0, h.sink.all(PULL).size(), "不得有任何写入");
        assertEquals(1, h.metrics.forSource(PULL).rejectedGlobal.get());

        // 许可释放后下一轮正常推进，数据恰好写入一次
        h.governor.release(PULL);
        assertEquals(3, h.coord.tick(PULL));
        assertEquals(3, h.offsets.current(PULL));
        assertEquals(3, h.sink.all(PULL).size());
        TestLog.because(scenario, "拉取与推送从同一份总容量取用；拒绝只计数不落地，恢复后续传不重复");
    }

    @Test
    void quotaHotUpdateAppliesImmediatelyAndKeepsAcceptedDataIntact(@TempDir Path tmp) throws Exception {
        String scenario = "quota-hot-update";
        Harness h = harness(tmp, 10);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", schema(), List.of()));
        TestLog.given(scenario, "start unlimited; then rate=1/s, then relax; invalid updates rejected");

        // 收紧限速到 1/s：立即生效
        h.quotas.update(PUSH, 1, null, null);
        h.svc.ingestBatch(PUSH, new RecordBatch("r1", null, List.of(rec("a"))));
        IngestionException limited = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("r2", null, List.of(rec("b")))));
        assertEquals(RejectReason.RATE_LIMITED, limited.reason());
        assertEquals(1, h.metrics.forSource(PUSH).rejectedRate.get());
        assertEquals(1, h.sink.all(PUSH).size(), "被限速的批次零写入");

        // 放宽限速：对此后的请求立即生效
        h.quotas.update(PUSH, 1000, null, null);
        var ok = h.svc.ingestBatch(PUSH, new RecordBatch("r2", null, List.of(rec("b"))));
        assertEquals(1, ok.accepted(), "热调整后同批次可立即通过，不丢数据");

        // 收紧单批规模：立即生效
        h.quotas.update(PUSH, null, null, 1);
        IngestionException tooLarge = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("r3", null,
                        List.of(rec("c"), rec("d")))));
        assertEquals(RejectReason.BATCH_TOO_LARGE, tooLarge.reason());
        assertEquals(2, h.sink.all(PUSH).size());

        // 非法配额：整体拒绝，当前生效配置不变
        var before = h.quotas.effective(PUSH);
        for (var bad : List.of(
                new Integer[]{-1, null, null},
                new Integer[]{null, 0, null},
                new Integer[]{null, null, 0})) {
            IngestionException invalid = assertThrows(IngestionException.class,
                    () -> h.quotas.update(PUSH, bad[0], bad[1], bad[2]));
            assertEquals(RejectReason.INVALID_QUOTA, invalid.reason());
        }
        var after = h.quotas.effective(PUSH);
        TestLog.actual(scenario, "effective before=" + before + " after=" + after);
        assertEquals(before, after, "非法调整不得改变生效配置");
        assertEquals(1000, after.rateLimitPerSec());
        assertEquals(1, after.batchSize());

        // 已接收数据不受任何调整影响
        assertEquals(2, h.sink.all(PUSH).size());
        TestLog.because(scenario, "配额存于 QuotaManager 并同步到 RateLimiter；"
                + "校验与生效同一临界区，非法即抛 INVALID_QUOTA 且原配置不变");
    }

    @Test
    void rejectionCountersStayAccurateAcrossReplayAndRedelivery(@TempDir Path tmp) throws Exception {
        String scenario = "reject-count-accuracy";
        Harness h = harness(tmp, 2);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", schema(), List.of()));
        var first = h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("k1"))));
        assertEquals(1, first.accepted());
        // 同键重复投递（新 batchId）：幂等识别为 DUPLICATE，不算拒绝
        var dup = h.svc.ingestBatch(PUSH, new RecordBatch("b2", null, List.of(rec("k1"))));
        assertEquals(1, dup.duplicate());
        TestLog.given(scenario, "1 accepted batch + 1 duplicate redelivery; "
                + "then replay under capacity pressure and force 1 global rejection");

        // 占住推送侧份额，让后续处于容量紧张环境
        assertTrue(h.governor.tryAcquire(PUSH));
        // 整批重放：在容量检查之前短路返回首次结果，不占用容量、不累计任何拒绝
        var replay = h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("k1"))));
        assertTrue(replay.replayed());
        // 真实拒绝恰好发生一次
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("b3", null, List.of(rec("k2")))));
        assertEquals(RejectReason.GLOBAL_CAPACITY_EXCEEDED, ex.reason());
        h.governor.release(PUSH);

        var c = h.metrics.forSource(PUSH);
        TestLog.actual(scenario, "rejected=" + c.rejected.get()
                + " rejectedGlobal=" + c.rejectedGlobal.get()
                + " rows=" + h.sink.all(PUSH).size());
        assertEquals(1, c.rejectedGlobal.get(), "重放/重复投递不得重复累计拒绝");
        assertEquals(1, c.rejected.get());
        assertEquals(1, h.sink.all(PUSH).size(), "只有真实接受的数据落盘，不丢不重");

        // 被拒绝的 b3 未入幂等库：容量释放后重投可正常处理
        var retried = h.svc.ingestBatch(PUSH, new RecordBatch("b3", null, List.of(rec("k2"))));
        assertFalse(retried.replayed());
        assertEquals(1, retried.accepted());
        assertEquals(2, h.sink.all(PUSH).size());
        TestLog.because(scenario, "重放在容量检查之前短路返回；被拒绝批次不落幂等结果，"
                + "拒绝计数只在真实拦截处累加一次");
    }
}
