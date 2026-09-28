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
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 场景：进程启动后未做任何运行期配额调整时，推送来源的按来源保护立即生效
 * （此前缺陷：推送桶只在第一次 PUT /quota 后才被创建，启动后连续突发批次直接穿透）。
 * 本类的装配刻意 <b>不</b> 手工 rl.register(...)，与生产装配路径一致：
 * 桶的唯一播种来源是 {@link QuotaService} 构造器。
 */
class StartupPerSourceProtectionTest {

    private static final String PUSH = "push-src";
    private static final String PULL = "pull-src";

    private record Harness(IngestionService svc, PullCoordinator pull, RecordSink sink,
                           OffsetStore offsets, MetricsStore metrics, RateLimiter rl,
                           QuotaService quotas, CapacityGovernor governor, Path fileDir) {
    }

    /**
     * 与生产一致的最小装配：不手工注册任何限速桶（修复前 PUSH 桶不存在 -> 全部放行）。
     */
    private Harness harness(Path tmp, int globalTotal, Integer pushRate, int pushBatchSize,
                            int pushInflight, int pullBatchSize) throws Exception {
        Path fileDir = tmp.resolve("filedir");
        Files.createDirectories(fileDir);
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp.resolve("state"));
        SourceConfig push = new SourceConfig();
        push.setName(PUSH);
        push.setType("PUSH");
        push.setToken("t1");
        push.setBatchSize(pushBatchSize);
        if (pushRate != null) {
            push.setRateLimitPerSec(pushRate);
        }
        push.setMaxInflight(pushInflight);
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
        // 刻意不调用 rl.register(...)：唯一播种路径应当是 QuotaService 构造器
        QuotaService quotas = new QuotaService(registry, rl);
        CapacityGovernor governor = new CapacityGovernor(globalTotal, 2);
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl, quotas, governor);
        PullCoordinator coord = new PullCoordinator(registry, svc, offsets, dlq, rl, metrics,
                quotas, governor, false);
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

    private MetricSnapshot snap(Harness h, String source) {
        return h.metrics.inMemorySnapshot(source, h.offsets.current(source), h.rl.inflight(source));
    }

    private void writePullLines(Path dir, int from, int to) throws Exception {
        List<String> lines = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            lines.add("{\"key\":\"p" + i + "\",\"data\":{\"id\":\"p" + i + "\"}}");
        }
        Files.write(dir.resolve("records-001.jsonl"),
                String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    }

    /** 启动后（无任何热调整）推送速率上限立即生效：满桶突发后后续批次 RATE_LIMITED。 */
    @Test
    void pushRateLimitAppliesImmediatelyAtStartup(@TempDir Path tmp) throws Exception {
        String scenario = "startup-push-rate-limit";
        Harness h = harness(tmp, 1000, 2, 100, 1000, 10);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        TestLog.given(scenario, "fresh process, rate=2/s, NO runtime quota update, no manual bucket register");

        // 满桶容量=2：连续两个不同批次各 1 条应当被接收
        h.svc.ingestBatch(PUSH, new RecordBatch("r1", null, List.of(rec("a"))));
        h.svc.ingestBatch(PUSH, new RecordBatch("r2", null, List.of(rec("b"))));
        // 第三个突发批次必须立即被限速拒绝（修复前此处会直接穿透接收）
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("r3", null, List.of(rec("c")))));
        MetricSnapshot s = snap(h, PUSH);
        TestLog.actual(scenario, "third burst -> " + ex.reason()
                + " rows=" + h.sink.all(PUSH).size()
                + " rejectedRateLimited=" + s.rejectedRateLimited());
        assertEquals(RejectReason.RATE_LIMITED, ex.reason(),
                "启动配置的速率上限在首次请求时即生效，不依赖运行期调整");
        assertEquals(2, h.sink.all(PUSH).size(), "被限速批次零写入");
        assertEquals(1, s.rejectedRateLimited(), "实际被拦 1 次 = 计数 1");
        assertEquals(0, s.rejectedBacklogFull());
        assertEquals(0, s.rejectedGlobalCapacity());
        TestLog.because(scenario, "QuotaService 构造期即为推送来源播种令牌桶，"
                + "速率限制与是否做过热调整无关");
    }

    /** 启动后推送在途上限立即生效：在途占满时新批次 BACKLOG_FULL。 */
    @Test
    void pushInflightLimitAppliesImmediatelyAtStartup(@TempDir Path tmp) throws Exception {
        String scenario = "startup-push-inflight";
        Harness h = harness(tmp, 1000, null, 100, 1, 10); // maxInflight=1
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        TestLog.given(scenario, "fresh process, maxInflight=1, the single slot is held");

        assertTrue(h.rl.tryAcquireInflight(PUSH)); // 外部占住唯一在途许可（模拟一个在途批次）
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("b", null, List.of(rec("z")))));
        MetricSnapshot s = snap(h, PUSH);
        TestLog.actual(scenario, ex.reason() + " rows=" + h.sink.all(PUSH).size()
                + " rejectedBacklogFull=" + s.rejectedBacklogFull());
        assertEquals(RejectReason.BACKLOG_FULL, ex.reason(),
                "启动配置的在途上限在首次请求时即生效");
        assertEquals(0, h.sink.all(PUSH).size(), "在途满拒绝零写入");
        assertEquals(1, s.rejectedBacklogFull());
        assertEquals(0, s.rejectedRateLimited());
        TestLog.because(scenario, "推送桶在启动播种时即携带配置的 maxInflight，占满即 BACKLOG_FULL");
    }

    /** 启动后单批规模上限立即生效（同样不依赖任何热调整）。 */
    @Test
    void pushBatchSizeLimitAppliesImmediatelyAtStartup(@TempDir Path tmp) throws Exception {
        String scenario = "startup-push-batch-size";
        Harness h = harness(tmp, 1000, null, 2, 1000, 10);
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        TestLog.given(scenario, "fresh process, batchSize=2, send 3 records");
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("big", null,
                        List.of(rec("a"), rec("b"), rec("c")))));
        assertEquals(RejectReason.BATCH_TOO_LARGE, ex.reason());
        assertEquals(0, h.sink.all(PUSH).size(), "超限整批拒绝，无部分写入");
        TestLog.because(scenario, "单批规模直接读启动生效配额，超限整批拒绝");
    }

    /** 推送与拉取共用总容量：启动配置下拉取轮次同样从同一全局池取许可。 */
    @Test
    void pushAndPullShareGlobalCapacityWithoutAnyAdjustment(@TempDir Path tmp) throws Exception {
        String scenario = "startup-shared-global-capacity";
        Harness h = harness(tmp, 1, null, 100, 1000, 10); // total=1
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        writePullLines(h.fileDir, 1, 4);
        TestLog.given(scenario, "fresh process, global total=1; push holds the only permit");

        assertTrue(h.governor.tryAcquire(PUSH));
        int processed = h.pull.tick(PULL); // 未调用过 start()，桶来自 QuotaService 播种
        MetricSnapshot pullSnap = snap(h, PULL);
        TestLog.actual(scenario, "pull processed=" + processed
                + " offset=" + pullSnap.offset()
                + " rejectedGlobalCapacity=" + pullSnap.rejectedGlobalCapacity());
        assertEquals(0, processed, "拉取被同一总容量拦下");
        assertEquals(0, pullSnap.offset(), "被拦不推进位点");
        assertEquals(0, h.sink.all(PULL).size(), "被拦零写入");
        assertEquals(1, pullSnap.rejectedGlobalCapacity());

        IngestionException pushEx = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(PUSH, new RecordBatch("b1", null, List.of(rec("a")))));
        assertEquals(RejectReason.GLOBAL_CAPACITY_EXHAUSTED, pushEx.reason());
        assertEquals(0, h.sink.all(PUSH).size());

        h.governor.release(PUSH);
        assertEquals(4, h.pull.tick(PULL), "释放后拉取恢复，数据不丢");
        TestLog.because(scenario, "总容量池在启动时即对推送/拉取两类来源同时生效");
    }

    /** 并发突发下不出现短时穿透：maxInflight 决定最多放行的在途批次数，拒绝数确定。 */
    @Test
    void concurrentBurstNeverPenetratesInflightBound(@TempDir Path tmp) throws Exception {
        String scenario = "startup-concurrent-burst";
        Harness h = harness(tmp, 1000, null, 100, 2, 10); // maxInflight=2，不限速
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger acceptedBatches = new AtomicInteger();
        AtomicInteger backlogRejects = new AtomicInteger();
        AtomicInteger otherErrors = new AtomicInteger();
        ConcurrentLinkedQueue<String> acceptedKeys = new ConcurrentLinkedQueue<>();
        TestLog.given(scenario, threads + " threads burst simultaneously, maxInflight=2");

        // 用两个屏障线程占住在途槽位，制造突发时槽位已满的窗口
        CountDownLatch holdersReady = new CountDownLatch(2);
        CountDownLatch releaseHolders = new CountDownLatch(1);
        for (int i = 0; i < 2; i++) {
            final int id = i;
            pool.submit(() -> {
                try {
                    h.rl.tryAcquireInflight(PUSH);
                    holdersReady.countDown();
                    releaseHolders.await();
                    h.rl.releaseInflight(PUSH);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        holdersReady.await(5, TimeUnit.SECONDS);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    BatchResult r = h.svc.ingestBatch(PUSH, new RecordBatch(
                            UUID.randomUUID().toString(), null,
                            List.of(rec("k-" + UUID.randomUUID()))));
                    acceptedBatches.incrementAndGet();
                    acceptedKeys.add(r.batchId());
                } catch (IngestionException e) {
                    if (e.reason() == RejectReason.BACKLOG_FULL) {
                        backlogRejects.incrementAndGet();
                    } else {
                        otherErrors.incrementAndGet();
                    }
                } catch (Exception e) {
                    otherErrors.incrementAndGet();
                }
            });
        }
        start.countDown();
        Thread.sleep(200);                 // 突发窗口内槽位始终被占
        releaseHolders.countDown();        // 之后放行，被拒批次可由客户端重试
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        MetricSnapshot s = snap(h, PUSH);
        TestLog.actual(scenario, "acceptedBatches=" + acceptedBatches.get()
                + " backlogRejects=" + backlogRejects.get()
                + " otherErrors=" + otherErrors.get()
                + " rejectedBacklogFull=" + s.rejectedBacklogFull());
        assertEquals(0, acceptedBatches.get(), "槽位占满期间任何突发批次都不得穿透");
        assertEquals(threads, backlogRejects.get(), "突发批次全部得到明确的 BACKLOG_FULL 拒绝");
        assertEquals(0, otherErrors.get(), "拒绝原因唯一、可区分");
        assertEquals(threads, s.rejectedBacklogFull(), "拒绝计数与实际被拦次数一致");
        assertEquals(0, h.sink.all(PUSH).size(), "全部被拒 -> 零写入");
        TestLog.because(scenario, "在途计数在同步块内检查并自增，并发下边界严格无穿透");
    }

    /** 被同一批次反复重放时不产生数据写入，也不累加拒绝计数（重放在容量门限之前识别）。 */
    @Test
    void replayAfterStartupEnforcementDoesNotInflateCounters(@TempDir Path tmp) throws Exception {
        String scenario = "startup-replay-counters";
        Harness h = harness(tmp, 1000, 1, 100, 1000, 10); // rate=1/s
        h.svc.ingestBatch(PUSH, new RecordBatch("init", pushSchema(), List.of()));
        TestLog.given(scenario, "rate=1/s; one ok batch replayed 3 times; one blocked batch retried 3 times");

        h.svc.ingestBatch(PUSH, new RecordBatch("ok", null, List.of(rec("a"))));
        for (int i = 0; i < 3; i++) {
            var replay = h.svc.ingestBatch(PUSH, new RecordBatch("ok", null, List.of(rec("a"))));
            assertTrue(replay.replayed());
        }
        for (int i = 0; i < 3; i++) {
            IngestionException ex = assertThrows(IngestionException.class,
                    () -> h.svc.ingestBatch(PUSH,
                            new RecordBatch("blocked", null, List.of(rec("x")))));
            assertEquals(RejectReason.RATE_LIMITED, ex.reason());
        }
        MetricSnapshot s = snap(h, PUSH);
        TestLog.actual(scenario, "rows=" + h.sink.all(PUSH).size()
                + " rejectedRateLimited=" + s.rejectedRateLimited() + " rejected=" + s.rejected());
        assertEquals(1, h.sink.all(PUSH).size(), "重放不产生重复写入");
        assertEquals(3, s.rejectedRateLimited(), "被拒批次每次实际重投计一次，重放不计");
        assertEquals(3, s.rejected());
        TestLog.because(scenario, "重放在容量扣减前直接返回首次结果；实际被拦次数与计数严格一致");
    }
}
