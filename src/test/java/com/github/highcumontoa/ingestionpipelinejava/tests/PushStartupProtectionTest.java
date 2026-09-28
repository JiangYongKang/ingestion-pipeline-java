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
import com.github.highcumontoa.ingestionpipelinejava.store.BatchIdempotencyStore;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 场景：进程刚启动、尚未做任何运行期配额调整时，推送来源（PUSH）的按来源保护
 * 必须与拉取来源一样立即生效——启动配置中的 rate-limit-per-sec / max-inflight /
 * batch-size 就是首个生效配额，连续突发批次不得被直接接收。
 *
 * <p>历史缺口：启动时只有 PullCoordinator 为拉取来源注册限速桶，推送来源要等首次
 * 热调整才建桶，而 RateLimiter 对无桶来源按“不限”处理，造成启动期穿透。
 * 修复后 QuotaService 构造时即为所有来源（推送+拉取）播种限速/积压桶。
 */
class PushStartupProtectionTest {

    private static final String SOURCE = "push-startup";

    private record Harness(IngestionService svc, QuotaService quotas, RateLimiter rl,
                           RecordSink sink, MetricsStore metrics) {
    }

    /** 按启动配置装配；测试全程不调用任何 quota 热调整。 */
    private Harness harness(Path tmp, int batchSize, int ratePerSec, int maxInflight) {
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp);
        SourceConfig c = new SourceConfig();
        c.setName(SOURCE);
        c.setType("PUSH");
        c.setToken("t");
        c.setBatchSize(batchSize);
        c.setRateLimitPerSec(ratePerSec);
        c.setMaxInflight(maxInflight);
        props.setSources(List.of(c));
        SourceRegistry registry = new SourceRegistry(props);
        SchemaStore schemas = new SchemaStore(props);
        SchemaValidator validator = new SchemaValidator(schemas);
        RecordSink sink = new RecordSink(props);
        var batches = new BatchIdempotencyStore(props);
        DeadLetterStore dlq = new DeadLetterStore(props);
        MetricsStore metrics = new MetricsStore(props);
        RateLimiter rl = new RateLimiter();
        // 生产装配路径：QuotaService 构造时按启动配置播种所有来源（不在此手工注册）
        QuotaService quotas = new QuotaService(registry, rl);
        CapacityGovernor governor = new CapacityGovernor(0, 1); // 本类聚焦按来源保护
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl, quotas, governor);
        return new Harness(svc, quotas, rl, sink, metrics);
    }

    private RecordSchema schema() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true)));
    }

    private IncomingRecord rec(String key) {
        return new IncomingRecord(key, new LinkedHashMap<>(Map.of("id", key)));
    }

    private MetricSnapshot snap(Harness h) {
        return h.metrics.inMemorySnapshot(SOURCE, 0L, h.rl.inflight(SOURCE));
    }

    @Test
    void startupConfigSeedsPushRateAndInflightLimitsWithoutAnyAdjustment(@TempDir Path tmp) {
        String scenario = "push-startup-seeded";
        Harness h = harness(tmp, 100, 5, 1);
        TestLog.given(scenario, "fresh process, no runtime quota update; push rate=5/s maxInflight=1");

        // 1) 启动配置即生效配额，且推送来源的限速/积压桶在任何请求之前已经就位
        var q = h.quotas.effective(SOURCE);
        TestLog.actual(scenario + "-quota", "effective quota at startup: " + q);
        assertEquals(5, q.rateLimitPerSec());
        assertEquals(1, q.maxInflight());
        assertTrue(h.rl.isRegistered(SOURCE),
                "推送来源必须在启动时（而非首次热调整后）就拥有生效中的限速/积压桶");

        // 2) 在途边界：maxInflight=1，唯一在途槽被占住时，第二个批次立即 BACKLOG_FULL
        h.svc.ingestBatch(SOURCE, new RecordBatch("init", schema(), List.of()));
        assertTrue(h.rl.tryAcquireInflight(SOURCE));
        IngestionException full = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(SOURCE,
                        new RecordBatch("hold", null, List.of(rec("z")))));
        TestLog.actual(scenario + "-inflight", full.reason().toString());
        assertEquals(RejectReason.BACKLOG_FULL, full.reason());
        assertEquals(0, h.sink.all(SOURCE).size(), "在途拒绝零写入");
        MetricSnapshot mid = snap(h);
        assertEquals(1, mid.rejectedBacklogFull());
        assertEquals(1, mid.rejected());
        h.rl.releaseInflight(SOURCE); // 归还占用，便于速率场景继续
    }

    @Test
    void startupRateLimitBlocksBurstImmediately(@TempDir Path tmp) throws Exception {
        String scenario = "push-startup-rate-burst";
        // 速率 10/s、桶容量 10、在途 1000（让在途完全不参与），全程不做任何热调整
        Harness h = harness(tmp, 100, 10, 1000);
        TestLog.given(scenario, "fresh process, rate=10/s, inflight=1000; 12 batches x 10 records burst");

        int threads = 12;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger acceptedBatches = new AtomicInteger();
        AtomicInteger rateLimited = new AtomicInteger();
        List<IncomingRecord> ten = List.of(rec("a"), rec("b"), rec("c"), rec("d"), rec("e"),
                rec("f"), rec("g"), rec("h"), rec("i"), rec("j"));
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    BatchResult r = h.svc.ingestBatch(SOURCE,
                            new RecordBatch(UUID.randomUUID().toString(), schema(), new ArrayList<>(ten)));
                    acceptedBatches.incrementAndGet();
                } catch (IngestionException e) {
                    if (e.reason() == RejectReason.RATE_LIMITED) {
                        rateLimited.incrementAndGet();
                    } else {
                        throw new AssertionError("unexpected reason " + e.reason(), e);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        MetricSnapshot s = snap(h);
        TestLog.actual(scenario, "acceptedBatches=" + acceptedBatches.get()
                + " rateLimitedBatches=" + rateLimited.get()
                + " rows=" + h.sink.all(SOURCE).size()
                + " rejectedRateLimited=" + s.rejectedRateLimited());
        // 令牌桶满桶 10、每批 10 条：恰好 1 批放行，其余突发全部被 RATE_LIMITED 拦截；
        // 并发下不允许出现“超过桶容量”的短时穿透。
        assertEquals(1, acceptedBatches.get(), "满桶只够一个 10 条批次");
        assertEquals(threads - 1, rateLimited.get(), "其余突发批次必须立即被限速拒绝");
        assertEquals(10, h.sink.all(SOURCE).size(), "被拒批次零写入，无部分写入");
        assertEquals(threads - 1, s.rejectedRateLimited());
        assertEquals(threads - 1, s.rejected());
        TestLog.because(scenario, "启动即播种的令牌桶（容量=rateLimitPerSec）在并发突发下原子扣减，"
                + "推送来源未做任何调整也受速率边界约束");
    }

    @Test
    void rejectedBatchProducesNoWriteAndItsRetryIsCountedOnce(@TempDir Path tmp) {
        String scenario = "push-startup-reject-redeliver";
        Harness h = harness(tmp, 100, 1, 1000);
        TestLog.given(scenario, "rate=1/s; first batch accepted, same unknown batchId redelivered 2x");

        BatchResult first = h.svc.ingestBatch(SOURCE,
                new RecordBatch("b1", schema(), List.of(rec("a"))));
        assertEquals(1, first.accepted());

        // 被拒批次不会落批次结果：用同一个新 batchId 连续重投两次，每次都是真实拦截、各计 1 次
        for (int i = 0; i < 2; i++) {
            IngestionException ex = assertThrows(IngestionException.class,
                    () -> h.svc.ingestBatch(SOURCE,
                            new RecordBatch("blocked", null, List.of(rec("x")))));
            assertEquals(RejectReason.RATE_LIMITED, ex.reason());
        }
        MetricSnapshot s = snap(h);
        assertEquals(2, s.rejectedRateLimited(), "每次真实被拦各计 1 次，不重不丢");
        assertEquals(2, s.rejected());
        assertEquals(1, h.sink.all(SOURCE).size(), "被拒重投不产生任何写入");

        // 已成功批次的整批重放：直接返回首次结果，拒绝计数不再增加
        BatchResult replay = h.svc.ingestBatch(SOURCE,
                new RecordBatch("b1", null, List.of(rec("a"))));
        assertTrue(replay.replayed());
        MetricSnapshot s2 = snap(h);
        assertEquals(2, s2.rejectedRateLimited(), "整批重放不累加拒绝计数");
        assertEquals(2, s2.rejected());
        assertEquals(1, h.sink.all(SOURCE).size(), "重放不产生重复写入");
        TestLog.because(scenario, "拒绝发生在批次幂等落盘之前：被拒批次重投按真实拦截计数，"
                + "成功批次重放走幂等返回，两者计数语义一致");
    }

    @Test
    void startupBatchSizeIsEnforcedBeforeAnyAdjustment(@TempDir Path tmp) {
        String scenario = "push-startup-batch-size";
        Harness h = harness(tmp, 2, 0, 1000);
        TestLog.given(scenario, "batchSize=2 at startup, no update; send 3");
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(SOURCE, new RecordBatch("big", schema(),
                        List.of(rec("a"), rec("b"), rec("c")))));
        assertEquals(RejectReason.BATCH_TOO_LARGE, ex.reason());
        assertEquals(0, h.sink.all(SOURCE).size());
        TestLog.because(scenario, "单批规模同样在启动时立即生效，不因来源类型被绕过");
    }
}
