package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceType;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 来源装配：按 ingestion.sources 装配多种来源（推送 / 文件拉取 / HTTP 拉取）。
 * 启动期强校验并明确拒绝：
 * <ul>
 *   <li>类型字符串无法识别 -> UNKNOWN_SOURCE_TYPE（400，与参数缺失区分）；</li>
 *   <li>名称/凭据/各类型必需项缺失 -> INCOMPLETE_CONFIG（400，明细列出缺失项）。</li>
 * </ul>
 * 运行期未知来源 -> UNKNOWN_SOURCE（404）。
 */
@Component
public class SourceRegistry {

    private static final Logger log = LoggerFactory.getLogger(SourceRegistry.class);

    private final Map<String, SourceDefinition> sources = new LinkedHashMap<>();

    public SourceRegistry(PipelineProperties props) {
        Set<String> names = new HashSet<>();
        List<SourceConfig> configs = props.getSources();
        for (SourceConfig c : configs) {
            if (c.getEnabled() != null && !c.getEnabled()) {
                continue;
            }
            validate(c, props, names);
            SourceDefinition def = build(c, props);
            sources.put(def.name(), def);
            names.add(def.name());
            log.info("source assembled: name={} type={} batchSize={} rateLimit={} maxInflight={} maxAttempts={}",
                    def.name(), def.type(), def.batchSize(), def.rateLimitPerSec(),
                    def.maxInflight(), def.maxAttempts());
        }
    }

    /** 校验单条配置，非法即抛出带可区分原因的异常（也供单元测试直接调用）。 */
    public static void validate(SourceConfig c, PipelineProperties props, Set<String> existingNames) {
        if (c == null) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG, "source entry is null");
        }
        if (c.getName() == null || c.getName().isBlank()) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "source 'name' is required");
        }
        if (existingNames != null && existingNames.contains(c.getName())) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "duplicated source name: " + c.getName());
        }
        if (c.getType() == null || c.getType().isBlank()) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "source '" + c.getName() + "' missing required config: type");
        }
        SourceType type;
        try {
            type = SourceType.parse(c.getType());
        } catch (IllegalArgumentException e) {
            throw new IngestionException(RejectReason.UNKNOWN_SOURCE_TYPE,
                    "source '" + c.getName() + "' has unknown type: " + c.getType()
                            + " (supported: PUSH, PULL_FILE, PULL_HTTP)");
        }
        if (c.getToken() == null || c.getToken().isBlank()) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "source '" + c.getName() + "' missing required config: token");
        }
        if (type == SourceType.PULL_FILE && (c.getDir() == null || c.getDir().isBlank())) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "source '" + c.getName() + "' missing required config for PULL_FILE: dir");
        }
        if (type == SourceType.PULL_HTTP && (c.getUrl() == null || c.getUrl().isBlank())) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "source '" + c.getName() + "' missing required config for PULL_HTTP: url");
        }
        if (c.getBatchSize() != null && c.getBatchSize() <= 0) {
            throw new IngestionException(RejectReason.INCOMPLETE_CONFIG,
                    "source '" + c.getName() + "' batch-size must be positive");
        }
    }

    private SourceDefinition build(SourceConfig c, PipelineProperties props) {
        SourceType type = SourceType.parse(c.getType());
        int batchSize = c.getBatchSize() != null ? c.getBatchSize() : props.getDefaultBatchSize();
        int rate = c.getRateLimitPerSec() != null ? c.getRateLimitPerSec() : 0;
        int inflight = c.getMaxInflight() != null ? c.getMaxInflight() : props.getDefaultMaxInflight();
        long poll = c.getPollIntervalMs() != null ? c.getPollIntervalMs() : props.getDefaultPollIntervalMs();
        int attempts = c.getMaxAttempts() != null ? c.getMaxAttempts() : props.getDefaultMaxAttempts();
        Path dir = c.getDir() == null ? null : Path.of(c.getDir());
        return new SourceDefinition(c.getName(), type, c.getToken(), dir, c.getUrl(),
                batchSize, rate, inflight, poll, attempts);
    }

    /** 查找来源，未知来源抛 UNKNOWN_SOURCE。 */
    public SourceDefinition require(String source) {
        SourceDefinition d = sources.get(source);
        if (d == null) {
            throw new IngestionException(RejectReason.UNKNOWN_SOURCE,
                    "unknown source: " + source);
        }
        return d;
    }

    public SourceDefinition find(String source) {
        return sources.get(source);
    }

    public Collection<SourceDefinition> all() {
        return List.copyOf(sources.values());
    }
}
