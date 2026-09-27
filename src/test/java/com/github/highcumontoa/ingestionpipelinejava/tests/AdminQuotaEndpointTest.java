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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

/**
 * 端到端：配额热调整与进程级容量观测的管理端点。
 * （独立 profile；@DirtiesContext 避免热调整的配额泄漏到共享上下文的其它测试类。）
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.config.location=classpath:application-test.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AdminQuotaEndpointTest {

    @LocalServerPort
    int port;

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

    @Test
    void quotaHotUpdateViaAdminEndpoint() {
        String scenario = "admin-quota-hot-update";
        // 1) 查询当前生效配额（启动配置）
        ResponseEntity<Map> q0 = rest.exchange(url("/api/admin/sources/wsrc/quota"),
                HttpMethod.GET, new HttpEntity<>(headers("wsrc-token")), Map.class);
        TestLog.given(scenario, "GET quota -> " + q0.getBody());
        assertEquals(HttpStatus.OK, q0.getStatusCode());
        assertEquals(100, ((Number) q0.getBody().get("batchSize")).intValue());

        // 2) 热调整 batchSize=1，立即对此后请求生效
        ResponseEntity<Map> q1 = rest.exchange(url("/api/admin/sources/wsrc/quota"),
                HttpMethod.PUT, new HttpEntity<>(Map.of("batchSize", 1), headers("wsrc-token")),
                Map.class);
        TestLog.actual(scenario, "PUT batchSize=1 -> " + q1.getStatusCode() + " " + q1.getBody());
        assertEquals(HttpStatus.OK, q1.getStatusCode());
        assertEquals(1, ((Number) q1.getBody().get("batchSize")).intValue());

        Map<String, Object> batch = new java.util.LinkedHashMap<>();
        batch.put("batchId", "q-too-big");
        batch.put("records", List.of(
                Map.of("key", "k1", "data", Map.of("id", "k1")),
                Map.of("key", "k2", "data", Map.of("id", "k2"))));
        ResponseEntity<Map> tooBig = rest.exchange(url("/api/sources/wsrc/ingest"),
                HttpMethod.POST, new HttpEntity<>(batch, headers("wsrc-token")), Map.class);
        assertEquals(HttpStatus.PAYLOAD_TOO_LARGE, tooBig.getStatusCode(),
                "新配额对此后请求立即生效");
        assertEquals("BATCH_TOO_LARGE", tooBig.getBody().get("reason"));

        // 3) 非法配额 -> 400 INVALID_QUOTA，生效配置不变
        ResponseEntity<Map> bad = rest.exchange(url("/api/admin/sources/wsrc/quota"),
                HttpMethod.PUT, new HttpEntity<>(Map.of("batchSize", 0), headers("wsrc-token")),
                Map.class);
        TestLog.actual(scenario + "-invalid", bad.getStatusCode() + " " + bad.getBody());
        assertEquals(HttpStatus.BAD_REQUEST, bad.getStatusCode());
        assertEquals("INVALID_QUOTA", bad.getBody().get("reason"));
        ResponseEntity<Map> q2 = rest.exchange(url("/api/admin/sources/wsrc/quota"),
                HttpMethod.GET, new HttpEntity<>(headers("wsrc-token")), Map.class);
        assertEquals(1, ((Number) q2.getBody().get("batchSize")).intValue(),
                "非法调整后生效配置保持不变");

        // 4) 跨来源访问配额 -> 403
        ResponseEntity<Map> cross = rest.exchange(url("/api/admin/sources/wsrc/quota"),
                HttpMethod.GET, new HttpEntity<>(headers("other-token")), Map.class);
        assertEquals(HttpStatus.FORBIDDEN, cross.getStatusCode());
        TestLog.because(scenario, "PUT 立即生效且可查询；非法值 400 且配置不变；来源隔离仍然成立");
    }

    @Test
    void globalCapacitySnapshotIsObservable() {
        String scenario = "admin-capacity-snapshot";
        ResponseEntity<Map> cap = rest.exchange(url("/api/admin/capacity"),
                HttpMethod.GET, new HttpEntity<>(headers("wsrc-token")), Map.class);
        TestLog.actual(scenario, cap.getStatusCode() + " " + cap.getBody());
        assertEquals(HttpStatus.OK, cap.getStatusCode());
        assertNotNull(cap.getBody().get("total"));
        assertNotNull(cap.getBody().get("fairShare"));
        assertNotNull(cap.getBody().get("perSourceInUse"));
        // 无凭据 -> 401
        ResponseEntity<Map> noAuth = rest.exchange(url("/api/admin/capacity"),
                HttpMethod.GET, new HttpEntity<>(headers(null)), Map.class);
        assertEquals(HttpStatus.UNAUTHORIZED, noAuth.getStatusCode());
        TestLog.because(scenario, "总容量/公平份额/各来源占用可查询，且仍受鉴权约束");
    }
}
