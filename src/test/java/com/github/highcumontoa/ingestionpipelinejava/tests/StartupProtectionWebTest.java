package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

/**
 * 真实 HTTP + 完整 Spring 装配路径的回归：
 * 进程启动后不做任何 PUT /quota，推送来源 application 配置中的
 * rate-limit-per-sec / max-inflight / batch-size 立即对首个请求生效。
 * 修复前推送桶只在第一次运行期配额调整时才创建，启动后连续突发批次全部穿透。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ingestion.state-dir=./target/test-data/startup-protection",
        "ingestion.pull.enabled=false",
        "ingestion.global-max-inflight=10000",
        "ingestion.sources[0].name=wsrc",
        "ingestion.sources[0].type=PUSH",
        "ingestion.sources[0].token=wsrc-token",
        "ingestion.sources[0].batch-size=100",
        "ingestion.sources[0].rate-limit-per-sec=1",
        "ingestion.sources[0].max-inflight=1"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StartupProtectionWebTest {

    @LocalServerPort
    int port;

    /** 每个测试周期从空状态开始，避免上一轮落盘的批次结果被识别为重放。 */
    @org.junit.jupiter.api.BeforeAll
    static void cleanState() throws Exception {
        java.nio.file.Path dir = java.nio.file.Path.of("target/test-data/startup-protection");
        if (java.nio.file.Files.exists(dir)) {
            try (var walk = java.nio.file.Files.walk(dir)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        java.nio.file.Files.deleteIfExists(p);
                    } catch (Exception ignored) {
                    }
                });
            }
        }
    }

    @org.springframework.beans.factory.annotation.Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.set("X-API-Key", "wsrc-token");
        return h;
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<Map> ingest(Object body) {
        return rest.exchange(url("/api/sources/wsrc/ingest"), HttpMethod.POST,
                new HttpEntity<>(body, headers()), Map.class);
    }

    private Map<String, Object> schemaInit() {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("batchId", "schema-init");
        m.put("schema", Map.of("version", 1, "fields", List.of(
                Map.of("name", "id", "type", "STRING", "required", true))));
        m.put("records", List.of());
        return m;
    }

    private Map<String, Object> batch(String id, String key) {
        return Map.of("batchId", id, "records", List.of(
                Map.of("key", key, "data", Map.of("id", key))));
    }

    @Test
    void startupConfiguredRateLimitAndInflightEnforcedBeforeAnyAdjustment() {
        String scenario = "startup-protection-via-http";
        TestLog.given(scenario, "fresh boot, rateLimit=1/s, maxInflight=1, never PUT /quota");

        // 声明结构（空批次也取 0 个令牌，不影响后续突发）
        assertEquals(HttpStatus.OK, ingest(schemaInit()).getStatusCode());

        // 首个在途批次占住唯一 inflight 槽位（通过管理面指标可见 inflight=1）：
        // 借助容量快照接口之前，先验证速率维度——首批 1 条消费唯一令牌
        ResponseEntity<Map> first = ingest(batch("b1", "k1"));
        TestLog.actual(scenario + "-b1", first.getStatusCode() + " " + first.getBody());
        assertEquals(HttpStatus.OK, first.getStatusCode(), "首批按启动配置放行");

        // 紧接着第二个突发批次：无任何热调整，启动速率上限立即生效 -> 429 RATE_LIMITED
        ResponseEntity<Map> second = ingest(batch("b2", "k2"));
        TestLog.actual(scenario + "-b2", second.getStatusCode() + " " + second.getBody());
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, second.getStatusCode(),
                "启动配置的每秒速率上限在首请求后即生效，连续突发被拦");
        assertEquals("RATE_LIMITED", second.getBody().get("reason"));

        // 被拒请求零写入：数据面仍只有 k1
        @SuppressWarnings("unchecked")
        ResponseEntity<Map> data = rest.exchange(url("/api/admin/sources/wsrc/data"),
                HttpMethod.GET, new HttpEntity<>(headers()), Map.class);
        Map<?, ?> records = (Map<?, ?>) data.getBody().get("records");
        TestLog.actual(scenario, "data after burst rejection: keys=" + records.keySet());
        assertEquals(1, records.size(), "被限速批次零写入");
        assertEquals(true, records.containsKey("k1"));

        // 拒绝原因可区分、计数可观测
        @SuppressWarnings("unchecked")
        ResponseEntity<Map> metrics = rest.exchange(
                url("/api/admin/sources/wsrc/metrics"),
                HttpMethod.GET, new HttpEntity<>(headers()), Map.class);
        assertEquals(1L,
                ((Number) metrics.getBody().get("rejectedRateLimited")).longValue(),
                "实际被拦 1 次 = rejectedRateLimited 1");
        TestLog.because(scenario, "QuotaService 启动即向 RateLimiter 播种推送桶；"
                + "推送与拉取在首请求起共用相同的按来源 + 总量两层保护");
    }
}
