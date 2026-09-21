package com.github.highcumontoa.ingestionpipelinejava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.dto.ErrorResponse;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 本地模拟“源端”HTTP 服务，供 PULL_HTTP 连接器拉取，演示真实拉取协议。
 *
 * <p>
 * <ul>
 *   <li>{@code POST /mock/sources/{sourceId}/feed}：向模拟源端投放一条记录，
 *       由源端分配单调位点（请求体即记录 JSON）；可带 X-Mock-Token 保护投放；</li>
 *   <li>{@code GET  /mock/sources/{sourceId}/records?after=n}：拉取协议，
 *       要求 {@code Authorization: Bearer <token>}，返回严格升序的记录。</li>
 * </ul>
 * 内存态随进程丢失，仅用于本地/测试模拟；真实环境替换为任意 HTTP 源即可。
 */
@RestController
public class MockSourceController {

    /** sourceId -> 该模拟源的令牌与数据（简单起见，令牌即来源注册令牌，测试可直接约定）。 */
    private final Map<String, MockSource> sources = new LinkedHashMap<>();

    private synchronized MockSource source(String sourceId, String expectedToken) {
        MockSource s = sources.computeIfAbsent(sourceId, k -> new MockSource());
        if (s.token == null) {
            s.token = expectedToken;
        }
        return s;
    }

    @PostMapping("/mock/sources/{sourceId}/feed")
    public synchronized Map<String, Object> feed(@PathVariable String sourceId,
                                                 @RequestHeader(value = "Authorization", required = false)
                                                 String authHeader,
                                                 @RequestBody JsonNode payload) {
        // 模拟源端也要求 Bearer 令牌：缺失/错误返回 401/403。
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            throw new IngestionException(ErrorCode.AUTH_MISSING_CREDENTIALS,
                    "mock source requires bearer token");
        }
        String token = authHeader.substring(7);
        MockSource s = source(sourceId, token);
        if (!token.equals(s.token)) {
            throw new IngestionException(ErrorCode.AUTH_FORBIDDEN,
                    "token does not authorize mock source '" + sourceId + "'");
        }
        if (payload == null || payload.isNull()) {
            throw new IngestionException(ErrorCode.PARAM_INVALID, "payload required");
        }
        long offset = ++s.nextOffset;
        s.records.put(offset, payload.deepCopy());
        return Map.of("sourceId", sourceId, "offset", offset);
    }

    @GetMapping("/mock/sources/{sourceId}/records")
    public synchronized ResponseEntity<?> records(@PathVariable String sourceId,
                                                  @RequestHeader(value = "Authorization", required = false)
                                                  String authHeader,
                                                  @org.springframework.web.bind.annotation.RequestParam
                                                  (defaultValue = "0") long after) {
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return ResponseEntity.status(401)
                    .body(new ErrorResponse(ErrorCode.AUTH_MISSING_CREDENTIALS,
                            "missing bearer token for mock source"));
        }
        String token = authHeader.substring(7);
        MockSource s = sources.get(sourceId);
        if (s == null || !token.equals(s.token)) {
            return ResponseEntity.status(403)
                    .body(new ErrorResponse(ErrorCode.AUTH_FORBIDDEN,
                            "forbidden for mock source '" + sourceId + "'"));
        }
        List<Map<String, Object>> out = new ArrayList<>();
        new TreeMap<>(s.records).forEach((offset, payload) -> {
            if (offset > after) {
                out.add(Map.of("offset", offset, "payload", payload));
            }
        });
        return ResponseEntity.ok(Map.of("records", out));
    }

    private static final class MockSource {
        String token;
        long nextOffset;
        final Map<Long, JsonNode> records = new LinkedHashMap<>();
    }
}
