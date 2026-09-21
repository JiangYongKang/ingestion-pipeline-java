package com.github.highcumontoa.ingestionpipelinejava.web;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.core.AuthService;
import com.github.highcumontoa.ingestionpipelinejava.core.IngestionService;
import com.github.highcumontoa.ingestionpipelinejava.core.PullRunner;
import com.github.highcumontoa.ingestionpipelinejava.error.RecordFailure;
import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaDefinition;
import com.github.highcumontoa.ingestionpipelinejava.model.SourceMetrics;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.ConflictRecord;
import com.github.highcumontoa.ingestionpipelinejava.store.DataStore;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetState;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import com.github.highcumontoa.ingestionpipelinejava.store.QuarantineStore;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import com.github.highcumontoa.ingestionpipelinejava.store.StoredRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 管理面端点（统一要求管理员 Bearer 令牌）：
 * 来源注册、结构注册/演进、位点与计数查询、数据/冲突查询、死信重试、手动拉取。
 */
@RestController
public class AdminController {

    private final AuthService auth;
    private final SourceRegistry registry;
    private final SchemaStore schemas;
    private final DataStore data;
    private final OffsetStore offsets;
    private final QuarantineStore quarantine;
    private final MetricsStore metrics;
    private final IngestionService ingestion;
    private final PullRunner pullRunner;

    public AdminController(AuthService auth, SourceRegistry registry, SchemaStore schemas,
                           DataStore data, OffsetStore offsets, QuarantineStore quarantine,
                           MetricsStore metrics, IngestionService ingestion,
                           PullRunner pullRunner) {
        this.auth = auth;
        this.registry = registry;
        this.schemas = schemas;
        this.data = data;
        this.offsets = offsets;
        this.quarantine = quarantine;
        this.metrics = metrics;
        this.ingestion = ingestion;
        this.pullRunner = pullRunner;
    }

    @PostMapping("/admin/sources")
    public SourceConfig registerSource(@RequestHeader("Authorization") String authHeader,
                                       @RequestBody SourceConfig config) {
        auth.requireAdmin(authHeader);
        registry.register(config);
        return config;
    }

    @GetMapping("/admin/sources")
    public List<SourceConfig> listSources(@RequestHeader("Authorization") String authHeader) {
        auth.requireAdmin(authHeader);
        return registry.all();
    }

    /** 首次注册结构（version 由服务端置为 1）。 */
    @PostMapping("/admin/sources/{sourceId}/schema")
    public SchemaDefinition registerSchema(@RequestHeader("Authorization") String authHeader,
                                           @PathVariable String sourceId,
                                           @RequestBody SchemaDefinition schema) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return schemas.initialize(sourceId, schema);
    }

    /** 结构演进：新增列（必须带确定 defaultValue）或删除列；版本自动 +1。 */
    @PostMapping("/admin/sources/{sourceId}/schema/evolve")
    public SchemaDefinition evolveSchema(@RequestHeader("Authorization") String authHeader,
                                         @PathVariable String sourceId,
                                         @RequestBody SchemaDefinition target) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return schemas.evolve(sourceId, target);
    }

    @GetMapping("/admin/sources/{sourceId}/offset")
    public OffsetState offset(@RequestHeader("Authorization") String authHeader,
                              @PathVariable String sourceId) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return offsets.load(sourceId);
    }

    @GetMapping("/admin/sources/{sourceId}/metrics")
    public SourceMetrics sourceMetrics(@RequestHeader("Authorization") String authHeader,
                                       @PathVariable String sourceId) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return metrics.get(sourceId);
    }

    @GetMapping("/admin/metrics")
    public List<SourceMetrics> allMetrics(@RequestHeader("Authorization") String authHeader) {
        auth.requireAdmin(authHeader);
        return metrics.all();
    }

    @GetMapping("/admin/sources/{sourceId}/data")
    public List<StoredRecord> data(@RequestHeader("Authorization") String authHeader,
                                   @PathVariable String sourceId) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return data.all(sourceId);
    }

    @GetMapping("/admin/sources/{sourceId}/conflicts")
    public List<ConflictRecord> conflicts(@RequestHeader("Authorization") String authHeader,
                                          @PathVariable String sourceId) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return data.conflicts(sourceId);
    }

    @GetMapping("/admin/sources/{sourceId}/quarantine")
    public List<RecordFailure> quarantineList(@RequestHeader("Authorization") String authHeader,
                                              @PathVariable String sourceId) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return quarantine.list(sourceId);
    }

    /**
     * 按策略重试单条隔离记录。请求体可省略（按原始内容重放），
     * 也可携带修复后的记录 JSON（运维纠错过录）。
     * 达到上限的记录返回 400，不会无限重投。
     */
    @PostMapping("/admin/sources/{sourceId}/quarantine/{businessKey}/retry")
    public BatchResult retry(@RequestHeader("Authorization") String authHeader,
                             @PathVariable String sourceId,
                             @PathVariable String businessKey,
                             @RequestBody(required = false) String fixedRawJson) {
        auth.requireAdmin(authHeader);
        registry.config(sourceId);
        return ingestion.retryOne(sourceId, businessKey, fixedRawJson);
    }

    /** 手动触发一次拉取周期（测试/本地验证用）。 */
    @PostMapping("/admin/pull/tick")
    public Map<String, String> pullTick(@RequestHeader("Authorization") String authHeader) {
        auth.requireAdmin(authHeader);
        pullRunner.runOnce();
        return Map.of("status", "done");
    }
}
