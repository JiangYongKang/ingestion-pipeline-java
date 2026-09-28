package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordBatch;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.QuotaService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

/**
 * 真实 Spring 装配冒烟：上下文启动完成后、在没有任何运行期配额调整的前提下，
 * 推送来源与拉取来源的限速/积压桶都已按启动配置就位，且推送速率边界立即生效。
 *
 * <p>该用例与手工装配的 {@link PushStartupProtectionTest} 互补：这里走的是
 * Spring 容器真正构造的 QuotaService/RateLimiter/IngestionService，
 * 防止“只有测试手工 register 才生效、生产装配仍漏注册推送源”的回归。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "ingestion.state-dir=./target/test-data/startup-wiring",
        "ingestion.pull.enabled=false",
        "ingestion.sources[0].name=wsrc",
        "ingestion.sources[0].type=PUSH",
        "ingestion.sources[0].token=wsrc-token",
        "ingestion.sources[0].batch-size=100",
        "ingestion.sources[0].rate-limit-per-sec=1",
        "ingestion.sources[0].max-inflight=2",
        "ingestion.sources[1].name=other",
        "ingestion.sources[1].type=PULL_FILE",
        "ingestion.sources[1].token=other-token",
        "ingestion.sources[1].dir=./target/test-data/startup-wiring/filedir",
        "ingestion.sources[1].batch-size=10"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StartupProtectionWiringTest {

    /**
     * 类初始化（早于 Spring 上下文加载）时清空固定状态目录，
     * 避免上一轮运行落盘的结构/批次结果以重放或版本冲突形式干扰本轮判定。
     */
    static {
        try {
            java.nio.file.Path dir = java.nio.file.Paths.get("target/test-data/startup-wiring");
            if (java.nio.file.Files.exists(dir)) {
                try (var walk = java.nio.file.Files.walk(dir)) {
                    walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                        try {
                            java.nio.file.Files.deleteIfExists(p);
                        } catch (java.io.IOException ignored) {
                            // 并发删除可忽略
                        }
                    });
                }
            }
        } catch (java.io.IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Autowired
    private RateLimiter rateLimiter;
    @Autowired
    private QuotaService quotaService;
    @Autowired
    private IngestionService ingestion;

    private RecordSchema schema() {
        return new RecordSchema(1, List.of(
                new SchemaField("id", SchemaField.FieldType.STRING, true)));
    }

    private IncomingRecord rec(String key) {
        return new IncomingRecord(key, new LinkedHashMap<>(Map.of("id", key)));
    }

    @Test
    void pushAndPullBucketsAreSeededAtStartupAndPushRateIsEnforcedImmediately() {
        String scenario = "startup-wiring-seeded";
        TestLog.given(scenario, "context refreshed, pull disabled, never PUT quota; "
                + "push wsrc rate=1/s inflight=2, pull other also assembled");

        // 1) 两类来源的限速/积压桶在启动后、任何请求之前均已就位（不依赖首次热调整）
        assertTrue(rateLimiter.isRegistered("wsrc"), "推送来源启动时必须已注册限速/积压桶");
        assertTrue(rateLimiter.isRegistered("other"), "拉取来源启动时同样已注册");
        QuotaService.Quota q = quotaService.effective("wsrc");
        TestLog.actual(scenario, "startup effective quota of wsrc: " + q);
        assertEquals(1, q.rateLimitPerSec());
        assertEquals(2, q.maxInflight());

        // 2) 真实 bean 链路上，推送来源首批消费唯一令牌，紧随的突发批次立即被限速拒绝。
        //    batchId 每次运行唯一，避免上一轮落盘的批次结果以“整批重放”跳过限速闸门。
        String uid = java.util.UUID.randomUUID().toString();
        ingestion.ingestBatch("wsrc", new RecordBatch("startup-init-" + uid, schema(), List.of()));
        ingestion.ingestBatch("wsrc", new RecordBatch("startup-r1-" + uid, null, List.of(rec("a"))));
        IngestionException ex = assertThrows(IngestionException.class,
                () -> ingestion.ingestBatch("wsrc",
                        new RecordBatch("startup-r2-" + uid, null, List.of(rec("b")))));
        TestLog.actual(scenario + "-burst", ex.reason().toString());
        assertEquals(RejectReason.RATE_LIMITED, ex.reason(),
                "进程刚启动、未做任何调整时，推送连续突发也必须被启动配置的速率上限拦截");
        TestLog.because(scenario, "QuotaService 是 Spring 启动期单例，构造时即为全部来源播种桶；"
                + "推送不再因缺少桶而被 RateLimiter 当作‘不限’放行");
    }
}
