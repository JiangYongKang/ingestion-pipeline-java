package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

/** 场景：并发写入最终一致、无重复；积压满与限速拒绝归类可区分且不产生写入。 */
class ConcurrencyAndBacklogTest {

    private static final String SOURCE = "conc";

    private record Harness(IngestionService svc, RecordSink sink, RateLimiter rl,
                           MetricsStore metrics, SourceRegistry registry,
                           PipelineProperties props) {
    }

    private Harness harness(Path tmp, int batchSize, Integer rate, int maxInflight) {
        PipelineProperties props = new PipelineProperties();
        props.setStateDir(tmp);
        SourceConfig c = new SourceConfig();
        c.setName(SOURCE);
        c.setType("PUSH");
        c.setToken("t");
        c.setBatchSize(batchSize);
        c.setRateLimitPerSec(rate);
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
        rl.register(SOURCE, maxInflight, rate == null ? 0 : rate);
        IngestionService svc = new IngestionService(registry, validator, schemas, sink,
                batches, dlq, metrics, rl);
        return new Harness(svc, sink, rl, metrics, registry, props);
    }

    private RecordSchema schema() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true),
                new SchemaField("v", SchemaField.FieldType.LONG, false)));
    }

    private IncomingRecord rec(String key, long v) {
        return new IncomingRecord(key, new LinkedHashMap<>(Map.of("id", key, "v", v)));
    }

    @Test
    void concurrentBatchesProduceExactlyOneRowPerKey(@TempDir Path tmp) throws Exception {
        String scenario = "concurrent-writes";
        Harness h = harness(tmp, 5000, null, 100000);
        h.svc.ingestBatch(SOURCE, new RecordBatch("schema-init", schema(), List.of()));
        int threads = 12;
        int keysPerThread = 80;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        TestLog.given(scenario, threads + " threads x " + keysPerThread
                + " unique keys, plus each key redelivered by a second batch (different batchId)");

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    List<IncomingRecord> first = new ArrayList<>();
                    List<IncomingRecord> redelivery = new ArrayList<>();
                    for (int i = 0; i < keysPerThread; i++) {
                        String key = "t" + tid + "-k" + i;
                        first.add(rec(key, i));
                        redelivery.add(rec(key, i)); // 同键同内容重复投递
                    }
                    var r1 = h.svc.ingestBatch(SOURCE,
                            new RecordBatch(UUID.randomUUID().toString(), null, first));
                    accepted.addAndGet(r1.accepted());
                    var r2 = h.svc.ingestBatch(SOURCE,
                            new RecordBatch(UUID.randomUUID().toString(), null, redelivery));
                    duplicate.addAndGet(r2.duplicate());
                } catch (Exception e) {
                    errors.incrementAndGet();
                    org.slf4j.LoggerFactory.getLogger("SCENARIO")
                            .error("[{}] unexpected: {}", scenario, e.toString());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        int expected = threads * keysPerThread;
        TestLog.actual(scenario, "rows=" + h.sink.all(SOURCE).size()
                + " accepted=" + accepted.get() + " duplicate=" + duplicate.get()
                + " errors=" + errors.get());
        assertEquals(0, errors.get());
        assertEquals(expected, h.sink.all(SOURCE).size(), "每个业务键最终恰好一行");
        assertEquals(expected, accepted.get(), "首批全部为新写入");
        assertEquals(expected, duplicate.get(), "重复投递全部幂等识别，无新增行");
        TestLog.because(scenario, "每来源串行化写入 + 内容哈希比较，并发下不产生重复行也不丢行");
    }

    @Test
    void backlogFullAndRateLimitAreRejectedWithDistinctReasons(@TempDir Path tmp) throws Exception {
        String scenario = "backlog-rate-limit";

        // 1) 有界积压：maxInflight=1，第二个在途请求必须被 BACKLOG_FULL 拒绝
        Harness h = harness(tmp.resolve("a"), 100, null, 1);
        h.svc.ingestBatch(SOURCE, new RecordBatch("init", schema(), List.of()));
        assertTrue(h.rl.tryAcquireInflight(SOURCE)); // 占用唯一许可
        TestLog.given(scenario, "maxInflight=1 and the single slot is held");
        IngestionException full = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(SOURCE,
                        new RecordBatch("b", null, List.of(rec("z", 1)))));
        TestLog.actual(scenario + "-backlog", full.reason() + " | " + full.getMessage());
        assertEquals(RejectReason.BACKLOG_FULL, full.reason());
        assertEquals(0, h.sink.all(SOURCE).size(), "积压拒绝不得产生任何写入");

        // 2) 限速：rate=1/s，首批消费令牌后第二批 RATE_LIMITED
        Harness h2 = harness(tmp.resolve("b"), 100, 1, 1000);
        h2.svc.ingestBatch(SOURCE, new RecordBatch("init", schema(), List.of()));
        TestLog.given(scenario, "rateLimit=1/s; send 1 record then immediately another batch");
        h2.svc.ingestBatch(SOURCE, new RecordBatch("b1", null, List.of(rec("a", 1))));
        IngestionException limited = assertThrows(IngestionException.class,
                () -> h2.svc.ingestBatch(SOURCE,
                        new RecordBatch("b2", null, List.of(rec("b", 1), rec("c", 1)))));
        TestLog.actual(scenario + "-rate", limited.reason() + " | " + limited.getMessage());
        assertEquals(RejectReason.RATE_LIMITED, limited.reason());
        TestLog.because(scenario, "BACKLOG_FULL(429 积压) 与 RATE_LIMITED(429 限速) reason 码不同，且均为零写入拒绝");
    }

    @Test
    void batchTooLargeIsRejectedBeforeAnyWrite(@TempDir Path tmp) {
        String scenario = "batch-too-large";
        Harness h = harness(tmp, 2, null, 100);
        h.svc.ingestBatch(SOURCE, new RecordBatch("init", schema(), List.of()));
        TestLog.given(scenario, "batchSize=2, sending 3 records");
        IngestionException ex = assertThrows(IngestionException.class,
                () -> h.svc.ingestBatch(SOURCE, new RecordBatch("big", null,
                        List.of(rec("a", 1), rec("b", 1), rec("c", 1)))));
        TestLog.actual(scenario, ex.reason() + " | " + ex.getMessage());
        assertEquals(RejectReason.BATCH_TOO_LARGE, ex.reason());
        assertEquals(0, h.sink.all(SOURCE).size());
        TestLog.because(scenario, "批量规模有界，超限整批拒绝且无部分写入");
    }
}
