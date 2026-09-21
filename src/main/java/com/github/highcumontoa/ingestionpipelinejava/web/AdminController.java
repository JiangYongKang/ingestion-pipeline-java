package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.model.DeadLetter;
import com.github.highcumontoa.ingestionpipelinejava.model.MetricSnapshot;
import com.github.highcumontoa.ingestionpipelinejava.service.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.service.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.service.RateLimiter;
import com.github.highcumontoa.ingestionpipelinejava.service.SourceDefinition;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.RecordSink;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 可观测/管理面。凭据必须有效，且只能访问凭据所属来源（跨来源 -> 403）。
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final AuthService auth;
    private final MetricsStore metrics;
    private final OffsetStore offsets;
    private final RateLimiter rateLimiter;
    private final DeadLetterStore dlq;
    private final RecordSink sink;
    private final SchemaStore schemas;
    private final IngestionService ingestion;

    public AdminController(AuthService auth, MetricsStore metrics, OffsetStore offsets,
                           RateLimiter rateLimiter, DeadLetterStore dlq, RecordSink sink,
                           SchemaStore schemas, IngestionService ingestion) {
        this.auth = auth;
        this.metrics = metrics;
        this.offsets = offsets;
        this.rateLimiter = rateLimiter;
        this.dlq = dlq;
        this.sink = sink;
        this.schemas = schemas;
        this.ingestion = ingestion;
    }

    /** 仅返回凭据归属来源的指标（来源隔离）。 */
    @GetMapping("/metrics")
    public Map<String, MetricSnapshot> metrics(
            @RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        SourceDefinition owner = auth.requireAnyValid(apiKey);
        return Map.of(owner.name(), snapshot(owner.name()));
    }

    private MetricSnapshot snapshot(String source) {
        return metrics.inMemorySnapshot(source, offsets.current(source), rateLimiter.inflight(source));
    }

    @GetMapping("/sources/{source}/metrics")
    public MetricSnapshot sourceMetrics(@PathVariable String source,
                                        @RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        auth.requireAccess(source, apiKey);
        return snapshot(source);
    }

    @GetMapping("/sources/{source}/offset")
    public Map<String, Object> offset(@PathVariable String source,
                                      @RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        auth.requireAccess(source, apiKey);
        return Map.of("source", source, "committedOffset", offsets.current(source));
    }

    /**
     * 为来源声明/演进结构（拉取来源没有批次结构通道，由此入口注册）。
     * 规则与推送批次内声明一致：版本只能 +1，新增字段必须可选（默认 null）。
     */
    @PostMapping("/sources/{source}/schema")
    public Map<String, Object> declareSchema(@PathVariable String source,
                                             @RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                             @RequestBody com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema schema) {
        auth.requireAccess(source, apiKey);
        var declared = schemas.register(source, schema);
        return Map.of("source", source, "version", declared.version(), "fields", declared.fields());
    }

    @GetMapping("/sources/{source}/dlq")
    public List<DeadLetter> dlq(@PathVariable String source,
                                @RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        auth.requireAccess(source, apiKey);
        return dlq.all(source);
    }

    /** 按策略重试 DLQ：每条最多 maxAttempts 次，超过即 EXHAUSTED，不无限重投。 */
    @PostMapping("/sources/{source}/dlq/retry")
    public Map<String, Integer> retryDlq(@PathVariable String source,
                                         @RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        auth.requireAccess(source, apiKey);
        int[] r = ingestion.retryPending(source);
        return Map.of("resolved", r[0], "stillFailed", r[1], "exhausted", r[2]);
    }

    /** 查看最终数据状态（冲突时保留首次写入内容）。 */
    @GetMapping("/sources/{source}/data")
    public Map<String, Object> data(@PathVariable String source,
                                    @RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        auth.requireAccess(source, apiKey);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("source", source);
        view.put("committedOffset", offsets.current(source));
        view.put("records", sink.all(source));
        return view;
    }
}
