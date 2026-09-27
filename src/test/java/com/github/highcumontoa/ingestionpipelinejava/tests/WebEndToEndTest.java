package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/**
 * 端到端：真实 HTTP 调用验证协议。使用独立测试 profile（临时状态目录、关闭拉取调度）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=classpath:application-test.properties"
        })
class WebEndToEndTest {

    @LocalServerPort
    int port;

    @org.junit.jupiter.api.BeforeAll
    static void cleanState() throws Exception {
        // 每个完整测试周期从空状态开始（持久化恢复由 PullFileSourceTest 单独验证）
        java.nio.file.Path dir = java.nio.file.Path.of("target/test-data/web-e2e");
        if (java.nio.file.Files.exists(dir)) {
            try (var walk = java.nio.file.Files.walk(dir)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                java.nio.file.Files.deleteIfExists(p);
                            } catch (Exception ignored) {
                            }
                        });
            }
        }
    }

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        if (token != null) {
            h.set("X-API-Key", token);
        }
        return h;
    }

    private Map<String, Object> schemaBody() {
        return Map.of("version", 1, "fields", List.of(
                Map.of("name", "id", "type", "STRING", "required", true),
                Map.of("name", "qty", "type", "LONG", "required", false)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> batch(String batchId, Object schema, List<?> records) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("batchId", batchId);
        if (schema != null) {
            m.put("schema", schema);
        }
        m.put("records", records);
        return m;
    }

    private Map<String, Object> record(String id, Object qty) {
        return Map.of("key", id, "data", new java.util.LinkedHashMap<>(Map.of("id", id, "qty", qty)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> post(String path, String token, Object body) {
        ResponseEntity<Map<String, Object>> resp = rest.exchange(url(path), HttpMethod.POST,
                new HttpEntity<>(body, headers(token)),
                new ParameterizedTypeReference<>() {
                });
        return resp.getBody();
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<Map<String, Object>> postRaw(String path, String token, Object body) {
        return rest.exchange(url(path), HttpMethod.POST,
                new HttpEntity<>(body, headers(token)),
                new ParameterizedTypeReference<>() {
                });
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<Map<String, Object>> get(String path, String token) {
        return rest.exchange(url(path), HttpMethod.GET,
                new HttpEntity<>(headers(token)),
                new ParameterizedTypeReference<>() {
                });
    }

    @Test
    void fullPushProtocolAuthIdempotencyMetricsAndDlqRetry() {
        String scenario = "web-e2e";
        String src = "wsrc";
        String tok = "wsrc-token";
        String ingest = "/api/sources/" + src + "/ingest";

        // 1) 无凭据：401 MISSING_CREDENTIAL
        var noCred = postRaw(ingest, null, batch("x", schemaBody(), List.of()));
        TestLog.given(scenario, "POST ingest without X-API-Key");
        TestLog.actual(scenario, "status=" + noCred.getStatusCode() + " body=" + noCred.getBody());
        assertEquals(401, noCred.getStatusCode().value());
        assertEquals("MISSING_CREDENTIAL", noCred.getBody().get("reason"));

        // 2) 错误凭据 / 跨来源凭据
        var badCred = postRaw(ingest, "wrong", batch("x", schemaBody(), List.of()));
        assertEquals(401, badCred.getStatusCode().value());
        assertEquals("INVALID_CREDENTIAL", badCred.getBody().get("reason"));
        var cross = postRaw(ingest, "other-token", batch("x", schemaBody(), List.of()));
        TestLog.actual(scenario, "cross-source status=" + cross.getStatusCode()
                + " reason=" + cross.getBody().get("reason"));
        assertEquals(403, cross.getStatusCode().value());
        assertEquals("CROSS_SOURCE_ACCESS", cross.getBody().get("reason"));

        // 未授权不得产生任何数据：data 接口鉴权后查看（此时无数据）
        var dataEmpty = get("/api/admin/sources/" + src + "/data", tok);
        Map<?, ?> recordsBefore = (Map<?, ?>) ((Map<?, ?>) dataEmpty.getBody()).get("records");
        TestLog.actual(scenario, "records after unauthorized attempts=" + recordsBefore.size());
        assertEquals(0, recordsBefore.size());

        // 3) 首批声明结构 + 2 条好数据 + 1 条类型不匹配坏数据
        var body = batch("b-1", schemaBody(), List.of(
                record("w1", 10L), record("w2", 20L), record("w3", "not-a-long")));
        var r1 = post(ingest, tok, body);
        TestLog.given(scenario, "batch b-1: w1 ok, w2 ok, w3 type mismatch");
        TestLog.actual(scenario, "result=" + r1);
        assertEquals(2, r1.get("accepted"));
        assertEquals(1, r1.get("quarantined"));

        // 4) 整批重放：replayed=true，数据不增加
        var replay = post(ingest, tok, body);
        TestLog.actual(scenario, "replay replayed=" + replay.get("replayed")
                + " accepted=" + replay.get("accepted"));
        assertEquals(Boolean.TRUE, replay.get("replayed"));

        // 5) 指标可观测
        var metrics = get("/api/admin/sources/" + src + "/metrics", tok).getBody();
        TestLog.actual(scenario, "metrics=" + metrics);
        assertEquals(2, ((Number) metrics.get("accepted")).intValue());
        assertEquals(1, ((Number) metrics.get("quarantined")).intValue());

        // 6) DLQ 可查询，保留原始数据与归类原因（接口直接返回数组）
        ResponseEntity<List> dlqResp = rest.exchange(url("/api/admin/sources/" + src + "/dlq"),
                HttpMethod.GET, new HttpEntity<>(headers(tok)),
                new ParameterizedTypeReference<List>() {
                });
        List<?> list = dlqResp.getBody();
        assertNotNull(list);
        assertEquals(1, list.size());
        Map<?, ?> dlqRow = (Map<?, ?>) list.get(0);
        TestLog.actual(scenario, "DLQ row reason=" + dlqRow.get("reason")
                + " data=" + dlqRow.get("data") + " detail=" + dlqRow.get("errorDetail"));
        assertEquals("TYPE_MISMATCH", dlqRow.get("reason"));
        assertEquals("w3", ((Map<?, ?>) dlqRow.get("data")).get("id"));

        // 7) 坏数据重试（数据仍旧非法）：首次失败 + 2 次重试后达到上限 EXHAUSTED，不无限重投；
        //    再次调用不再重投已耗尽记录；已有数据不受污染
        var retry1 = post("/api/admin/sources/" + src + "/dlq/retry", tok, Map.of());
        TestLog.actual(scenario, "retry#1=" + retry1);
        assertEquals(1, ((Number) retry1.get("stillFailed")).intValue());
        var retry2 = post("/api/admin/sources/" + src + "/dlq/retry", tok, Map.of());
        TestLog.actual(scenario, "retry#2=" + retry2);
        assertEquals(1, ((Number) retry2.get("exhausted")).intValue());
        var retry3 = post("/api/admin/sources/" + src + "/dlq/retry", tok, Map.of());
        TestLog.actual(scenario, "retry#3(no more redelivery)=" + retry3);
        assertEquals(0, ((Number) retry3.get("exhausted")).intValue());
        assertEquals(0, ((Number) retry3.get("stillFailed")).intValue());

        // 8) 最终状态可解释：2 条好数据完好
        var data = get("/api/admin/sources/" + src + "/data", tok).getBody();
        Map<?, ?> finalRecords = (Map<?, ?>) data.get("records");
        TestLog.actual(scenario, "FINAL records=" + finalRecords.keySet());
        assertEquals(2, finalRecords.size());
        assertTrue(finalRecords.containsKey("w1") && finalRecords.containsKey("w2"));
        TestLog.because(scenario, "401/403 与业务拒绝可区分且零写入；坏数据隔离+有界重试；最终状态只含合法数据");
    }

    @Test
    void quotaEndpointsExposeAndHotUpdateEffectiveQuota() {
        String scenario = "web-quota";
        String src = "other";
        String tok = "other-token";
        String capacity = "/api/admin/sources/" + src + "/capacity";
        String quota = "/api/admin/sources/" + src + "/quota";

        // 1) 初始生效配额可查询（来自装配配置），拒绝计数为 0
        var c0 = get(capacity, tok);
        TestLog.given(scenario, "GET capacity for freshly assembled source");
        TestLog.actual(scenario, "status=" + c0.getStatusCode() + " body=" + c0.getBody());
        assertEquals(200, c0.getStatusCode().value());
        Map<String, Object> q0 = (Map<String, Object>) c0.getBody().get("quota");
        assertEquals(0, ((Number) q0.get("rateLimitPerSec")).intValue());
        Map<String, Object> rej0 = (Map<String, Object>) c0.getBody().get("rejections");
        assertEquals(0, ((Number) rej0.get("globalCapacityExceeded")).longValue());
        assertNotNull(c0.getBody().get("global"));

        // 2) 热调整：只改速率，其它字段保持
        var put = rest.exchange(url(quota), HttpMethod.PUT,
                new HttpEntity<>(Map.of("rateLimitPerSec", 7), headers(tok)), Map.class);
        assertEquals(200, put.getStatusCode().value());
        Map<String, Object> q1 = (Map<String, Object>) put.getBody().get("quota");
        TestLog.actual(scenario, "after PUT rateLimitPerSec=7 -> " + q1);
        assertEquals(7, ((Number) q1.get("rateLimitPerSec")).intValue());
        assertEquals(q0.get("maxInflight"), q1.get("maxInflight"));
        assertEquals(q0.get("batchSize"), q1.get("batchSize"));

        // 3) 调整后的生效情况可查询
        var c1 = get(capacity, tok);
        Map<String, Object> q1q = (Map<String, Object>) c1.getBody().get("quota");
        assertEquals(7, ((Number) q1q.get("rateLimitPerSec")).intValue());

        // 4) 非法配额：400 INVALID_QUOTA，生效配置不变
        var bad = rest.exchange(url(quota), HttpMethod.PUT,
                new HttpEntity<>(Map.of("maxInflight", 0), headers(tok)), Map.class);
        TestLog.actual(scenario, "invalid PUT status=" + bad.getStatusCode() + " body=" + bad.getBody());
        assertEquals(400, bad.getStatusCode().value());
        assertEquals("INVALID_QUOTA", bad.getBody().get("reason"));
        var c2 = get(capacity, tok);
        assertEquals(c1.getBody().get("quota"), c2.getBody().get("quota"), "非法调整后生效配置不变");

        // 5) 无凭据调整：401，零副作用
        var noCred = rest.exchange(url(quota), HttpMethod.PUT,
                new HttpEntity<>(Map.of("rateLimitPerSec", 1), headers(null)), Map.class);
        assertEquals(401, noCred.getStatusCode().value());
        TestLog.because(scenario, "配额热调整经管理面生效并可查询；非法值与未授权请求均被拒绝且不改配置");
    }
}
