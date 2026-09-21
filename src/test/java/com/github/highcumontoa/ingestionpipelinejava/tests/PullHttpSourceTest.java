package com.github.highcumontoa.ingestionpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.springframework.http.ResponseEntity;
import org.springframework.core.ParameterizedTypeReference;

/**
 * 场景：HTTP 拉取。通过 mock 端点预置数据，直接使用 HttpSourceReader + 拉取处理链路，
 * 验证携凭据拉取、连续序号处理与位点推进。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.config.location=classpath:application-test.properties"})
class PullHttpSourceTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.service.SourceRegistry registry;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.service.IngestionService ingestion;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore offsets;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore dlq;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter rateLimiter;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore metrics;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore schemas;
    @Autowired
    com.github.highcumontoa.ingestionpipelinejava.store.RecordSink sink;

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-API-Key", token);
        return h;
    }

    @Test
    void httpPullReadsContinuousSequenceAndCommitsOffset() throws Exception {
        String scenario = "pull-http";
        // 复用 PUSH 来源 wsrc 作为被拉取的模拟来源（mock 端点按来源名鉴权）
        String source = "pull-wsrc";
        String token = "pull-wsrc-token";
        String base = "http://localhost:" + port + "/mock/sources/" + source;

        // 预置 3 条记录到 mock 源端
        List<Map<String, Object>> seed = List.of(
                Map.of("key", "h1", "data", Map.of("id", "h1", "qty", 1L)),
                Map.of("key", "h2", "data", Map.of("id", "h2", "qty", 2L)),
                Map.of("key", "h3", "data", Map.of("id", "h3", "qty", 3L)));
        TestLog.given(scenario, "seed mock http source with 3 records: " + seed);
        ResponseEntity<Map> seeded = rest.exchange(base + "/seed", HttpMethod.POST,
                new HttpEntity<>(seed, headers(token)), Map.class);
        assertEquals(200, seeded.getStatusCode().value());

        // 确保结构存在（测试 profile 下不跑调度，手工声明 v1 与 wsrc 推送接口同构）
        if (!schemas.exists(source)) {
            schemas.register(source, new com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema(1,
                    List.of(new com.github.highcumontoa.ingestionpipelinejava.model.SchemaField(
                                    "id", com.github.highcumontoa.ingestionpipelinejava.model.SchemaField.FieldType.STRING, true),
                            new com.github.highcumontoa.ingestionpipelinejava.model.SchemaField(
                                    "qty", com.github.highcumontoa.ingestionpipelinejava.model.SchemaField.FieldType.LONG, false))));
        }

        // 直接驱动协调器一轮，但读取器指向本地 mock 端点
        var coordinator = new com.github.highcumontoa.ingestionpipelinejava.service.pull.PullCoordinator(
                registry, ingestion, offsets, dlq, rateLimiter, metrics, false);
        coordinator.registerReader(source,
                new com.github.highcumontoa.ingestionpipelinejava.service.pull.HttpSourceReader(base, token));

        int n = coordinator.tick(source);
        TestLog.actual(scenario, "tick processed=" + n + " offset=" + offsets.current(source));
        // 注意：wsrc 在 WebEndToEndTest 之外独立运行，offset 从 0 起（该测试类独立上下文与状态目录）
        assertEquals(3, n, "三条连续记录必须全部处理");
        assertEquals(3, offsets.current(source));
        assertEquals(0, dlq.all(source).size(), "全部合法，无隔离");
        TestLog.because(scenario, "HTTP 读取携带 X-API-Key，序号 1..3 连续处理后位点提交到 3");

        // 再 tick 无数据 -> 不推进、不重放
        int n2 = coordinator.tick(source);
        TestLog.actual(scenario, "second tick processed=" + n2 + " offset=" + offsets.current(source));
        assertEquals(0, n2);
        assertEquals(3, offsets.current(source));
    }

    @Test
    void httpPullWithoutCredentialIsRejectedByMockEndpoint() {
        String scenario = "pull-http-auth";
        String base = "http://localhost:" + port + "/mock/sources/pull-wsrc/records?after=0&limit=10";
        TestLog.given(scenario, "GET mock records without X-API-Key");
        ResponseEntity<Map> resp = rest.exchange(base, HttpMethod.GET,
                new HttpEntity<>(new HttpHeaders()),
                new ParameterizedTypeReference<Map>() {
                });
        TestLog.actual(scenario, "status=" + resp.getStatusCode() + " body=" + resp.getBody());
        assertEquals(401, resp.getStatusCode().value());
        assertEquals("MISSING_CREDENTIAL", resp.getBody().get("reason"));
        TestLog.because(scenario, "拉取源端点同样强制鉴权，未授权不返回数据");
    }
}
