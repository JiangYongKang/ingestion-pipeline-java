package com.github.highcumontoa.ingestionpipelinejava.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 拉取式 HTTP 来源（本地用内置模拟端点实现）。
 *
 * <p>协议：{@code GET <baseUrl>/records?after=<offset>}，携带
 * {@code Authorization: Bearer <token>}；响应 JSON：
 * {@code {"records":[{"offset": 12, "payload": {...}}, ...]}}，按位点升序。
 * 来源端 401/403 透传为鉴权错误；其它非 2xx 归类为参数/来源错误，不写入、不推进。
 */
public class HttpSourceConnector implements SourceConnector {

    private final SourceConfig config;
    private final HttpClient client;

    public HttpSourceConnector(SourceConfig config) {
        this.config = config;
        // 显式提供有界执行器：不依赖默认 HttpClient 调度器（其线程为守护线程且数量不受控），
        // 同时避免在 Web 请求线程内自调用本地端点时的调度饥饿。
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .executor(java.util.concurrent.Executors.newFixedThreadPool(4, r -> {
                    Thread t = new Thread(r, "http-source-client");
                    t.setDaemon(true);
                    return t;
                }))
                .build();
    }

    @Override
    public SourceConfig config() {
        return config;
    }

    @Override
    public boolean pullable() {
        return true;
    }

    @Override
    public PollBatch poll(long afterOffset) {
        try {
            HttpRequest.Builder rb = HttpRequest.newBuilder()
                    .uri(URI.create(config.getBaseUrl() + "/records?after=" + afterOffset))
                    .timeout(Duration.ofSeconds(5))
                    .GET();
            if (config.getToken() != null && !config.getToken().isBlank()) {
                rb.header("Authorization", "Bearer " + config.getToken());
            }
            HttpResponse<String> resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 401) {
                throw new IngestionException(ErrorCode.AUTH_MISSING_CREDENTIALS,
                        "source endpoint rejected missing/invalid credentials for '" + config.getId() + "'");
            }
            if (resp.statusCode() == 403) {
                throw new IngestionException(ErrorCode.AUTH_FORBIDDEN,
                        "source endpoint forbids access for '" + config.getId() + "'");
            }
            if (resp.statusCode() / 100 != 2) {
                throw new IngestionException(ErrorCode.PARAM_INVALID,
                        "source endpoint returned HTTP " + resp.statusCode() + " for '" + config.getId()
                                + "', body=" + resp.body());
            }
            JsonNode root = JsonFiles.mapper().readTree(resp.body());
            JsonNode arr = root.path("records");
            List<SourceRecord> records = new ArrayList<>();
            for (JsonNode item : arr) {
                long offset = item.path("offset").asLong();
                JsonNode payload = item.path("payload");
                records.add(new SourceRecord(offset, JsonFiles.mapper().writeValueAsString(payload)));
            }
            return new PollBatch(records);
        } catch (IngestionException e) {
            throw e;
        } catch (Exception e) {
            throw new IngestionException(ErrorCode.PARAM_INVALID,
                    "failed to poll http source '" + config.getId() + "': "
                            + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }
}
