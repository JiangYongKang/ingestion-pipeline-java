package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 场景：缺凭据(401)、凭据无效(401)、跨来源访问(403)与参数错误(400)可区分；
 * 未授权请求不产生任何写入或位点变化；来源类型未知与配置不完整被明确拒绝。
 */
class AuthAndConfigTest extends IngestionTestBase {

    private String sidA;
    private String sidB;
    private String tokenA;
    private String tokenB;

    @BeforeEach
    void setup() {
        sidA = "auth-a-" + System.nanoTime();
        sidB = "auth-b-" + System.nanoTime();
        tokenA = "token-A-" + System.nanoTime();
        tokenB = "token-B-" + System.nanoTime();
        registerSource(sidA, "PUSH", tokenA, null);
        registerSource(sidB, "PUSH", tokenB, null);
        registerBasicSchema(sidA);
        registerBasicSchema(sidB);
    }

    private ObjectNode oneRecord() {
        return batch(List.of(item(1L, record("u1", "alice", 20))));
    }

    @Test
    @DisplayName("缺少 Authorization 头 => 401 AUTH_MISSING_CREDENTIALS，且无写入无位点变化")
    void missingCredentials() {
        HttpHeaders noAuth = new HttpHeaders();
        ResponseEntity<JsonNode> r = rest.exchange(base() + "/api/sources/" + sidA + "/records",
                HttpMethod.POST, new HttpEntity<>(oneRecord(), noAuth), JsonNode.class);
        log.info("[INPUT] no auth header -> status={}, body={}", r.getStatusCode(), r.getBody());
        assertEquals(401, r.getStatusCode().value());
        assertEquals("AUTH_MISSING_CREDENTIALS", r.getBody().get("code").asText());
        assertUntouched(sidA);
    }

    @Test
    @DisplayName("令牌不属于任何来源 => 401 AUTH_INVALID_CREDENTIALS")
    void invalidCredentials() {
        ResponseEntity<JsonNode> r = push(sidA, "totally-wrong-"+System.nanoTime(), oneRecord());
        log.info("[INPUT] bogus token -> status={}, code={}", r.getStatusCode(),
                r.getBody().get("code").asText());
        assertEquals(401, r.getStatusCode().value());
        assertEquals("AUTH_INVALID_CREDENTIALS", r.getBody().get("code").asText());
        assertUntouched(sidA);
    }

    @Test
    @DisplayName("用 A 的令牌访问 B => 403 AUTH_FORBIDDEN（跨来源隔离）")
    void crossSourceForbidden() {
        ResponseEntity<JsonNode> r = push(sidB, tokenA, oneRecord());
        log.info("[INPUT] token-A -> source {} => status={}, code={}",
                sidB, r.getStatusCode(), r.getBody().get("code").asText());
        assertEquals(403, r.getStatusCode().value());
        assertEquals("AUTH_FORBIDDEN", r.getBody().get("code").asText());
        assertUntouched(sidB);

        // 正确令牌仍然可用，且只写入自己的来源
        ResponseEntity<JsonNode> ok = push(sidA, tokenA, oneRecord());
        ensure2xx(ok, "token-A to source A");
        assertEquals(1, getAdmin("/admin/sources/" + sidA + "/data").size());
        assertEquals(0, getAdmin("/admin/sources/" + sidB + "/data").size());
    }

    @Test
    @DisplayName("鉴权错误(401/403)与参数错误(400) HTTP 语义可区分")
    void authErrorsDistinctFromParamErrors() {
        // 空请求体：即使带了正确令牌，参数问题为 400 而非 401
        ObjectNode empty = JsonFiles.mapper().createObjectNode();
        empty.putArray("records");
        ResponseEntity<JsonNode> r = push(sidA, tokenA, empty);
        log.info("[INPUT] empty records -> status={}, code={}", r.getStatusCode(),
                r.getBody().get("code").asText());
        assertEquals(400, r.getStatusCode().value());

        // 无令牌访问不存在的来源：鉴权先发生，仍为 401（不泄漏来源是否存在）
        HttpHeaders noAuth = new HttpHeaders();
        ResponseEntity<JsonNode> r2 = rest.exchange(
                base() + "/api/sources/does-not-exist/records",
                HttpMethod.POST, new HttpEntity<>(oneRecord(), noAuth), JsonNode.class);
        assertEquals(401, r2.getStatusCode().value());
    }

    @Test
    @DisplayName("未知来源类型 => SOURCE_TYPE_UNKNOWN；不完整配置 => SOURCE_CONFIG_INVALID")
    void unknownTypeAndInvalidConfigRejected() {
        ObjectNode unknown = JsonFiles.mapper().createObjectNode();
        unknown.put("id", "bad-type");
        unknown.put("type", "KAFKA");
        unknown.put("token", "x");
        ResponseEntity<JsonNode> r = adminPostRaw("/admin/sources", unknown);
        log.info("[INPUT] unknown type -> status={}, code={}, msg={}",
                r.getStatusCode(), code(r), r.getBody().get("message").asText());
        assertEquals(400, r.getStatusCode().value());
        assertEquals("SOURCE_TYPE_UNKNOWN", code(r));

        ObjectNode missingToken = JsonFiles.mapper().createObjectNode();
        missingToken.put("id", "no-token");
        missingToken.put("type", "PUSH");
        ResponseEntity<JsonNode> r2 = adminPostRaw("/admin/sources", missingToken);
        log.info("[INPUT] missing token -> code={}", code(r2));
        assertEquals("SOURCE_CONFIG_INVALID", code(r2));

        ObjectNode missingWatchDir = JsonFiles.mapper().createObjectNode();
        missingWatchDir.put("id", "no-watch");
        missingWatchDir.put("type", "PULL_FILE");
        missingWatchDir.put("token", "x");
        ResponseEntity<JsonNode> r3 = adminPostRaw("/admin/sources", missingWatchDir);
        log.info("[INPUT] PULL_FILE without watchDir -> code={}, msg={}",
                code(r3), r3.getBody().get("message").asText());
        assertEquals("SOURCE_CONFIG_INVALID", code(r3));

        ObjectNode missingBaseUrl = JsonFiles.mapper().createObjectNode();
        missingBaseUrl.put("id", "no-url");
        missingBaseUrl.put("type", "PULL_HTTP");
        missingBaseUrl.put("token", "x");
        ResponseEntity<JsonNode> r4 = adminPostRaw("/admin/sources", missingBaseUrl);
        assertEquals("SOURCE_CONFIG_INVALID", code(r4));
    }

    @Test
    @DisplayName("管理端点：错误/缺失管理员令牌被拒绝（401/403）")
    void adminTokenEnforced() {
        HttpHeaders wrong = new HttpHeaders();
        wrong.setBearerAuth("not-admin");
        ResponseEntity<JsonNode> r = rest.exchange(base() + "/admin/metrics",
                HttpMethod.GET, new HttpEntity<>(wrong), JsonNode.class);
        log.info("[INPUT] wrong admin token -> status={}", r.getStatusCode());
        assertEquals(401, r.getStatusCode().value());

        HttpHeaders none = new HttpHeaders();
        ResponseEntity<JsonNode> r2 = rest.exchange(base() + "/admin/metrics",
                HttpMethod.GET, new HttpEntity<>(none), JsonNode.class);
        assertEquals(401, r2.getStatusCode().value());
    }

    private void assertUntouched(String sourceId) {
        JsonNode offset = getAdmin("/admin/sources/" + sourceId + "/offset");
        JsonNode data = getAdmin("/admin/sources/" + sourceId + "/data");
        log.info("[STATE] unauthorized attempt leaves offset={}, rows={}",
                offset.get("offset").asLong(), data.size());
        assertEquals(0, offset.get("offset").asLong(), "未授权请求不得推进位点");
        assertEquals(0, data.size(), "未授权请求不得写入数据");
    }

    private String code(ResponseEntity<JsonNode> r) {
        return r.getBody().path("code").asText("null");
    }

    private ResponseEntity<JsonNode> adminPostRaw(String path, ObjectNode body) {
        return rest.exchange(base() + path, HttpMethod.POST,
                new HttpEntity<>(body, adminHeaders()), JsonNode.class);
    }
}
