package com.github.highcumontoa.ingestionpipelinejava.source;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 来源装配与运行时注册表。
 *
 * <p>装配来源：启动配置 {@code ingestion.sources[*]} + 数据目录中持久化的
 * {@code sources.json}（动态注册结果，重启后自动恢复装配）。
 * 未知类型与不完整配置在装配时即被拒绝，原因码不同。
 */
public class SourceRegistry {

    private final Path persistFile;
    private final Map<String, Entry> entries = new LinkedHashMap<>();

    private record Entry(SourceConfig config, SourceConnector connector) {
    }

    public SourceRegistry(PipelineProperties props, Path dataDir) {
        this.persistFile = dataDir.resolve("sources.json");
        for (SourceConfig cfg : props.getSources()) {
            register(cfg, false);
        }
        if (Files.exists(persistFile)) {
            Registrations persisted = JsonFilesReader.read(persistFile);
            for (SourceConfig cfg : persisted.sources) {
                if (!entries.containsKey(cfg.getId())) {
                    register(cfg, false);
                }
            }
        }
    }

    /** 动态注册并持久化；重复 id 拒绝。 */
    public synchronized SourceConnector register(SourceConfig cfg) {
        return register(cfg, true);
    }

    private synchronized SourceConnector register(SourceConfig cfg, boolean persist) {
        if (entries.containsKey(cfg.getId())) {
            throw new IngestionException(ErrorCode.PARAM_INVALID,
                    "source '" + cfg.getId() + "' already registered");
        }
        SourceConnector connector = SourceConnectorFactory.create(cfg);
        entries.put(cfg.getId(), new Entry(cfg, connector));
        if (persist) {
            save();
        }
        return connector;
    }

    public synchronized SourceConfig config(String sourceId) {
        Entry e = entries.get(sourceId);
        if (e == null) {
            throw new IngestionException(ErrorCode.SOURCE_NOT_FOUND,
                    "source '" + sourceId + "' is not registered");
        }
        return e.config();
    }

    public synchronized SourceConnector require(String sourceId) {
        Entry e = entries.get(sourceId);
        if (e == null) {
            throw new IngestionException(ErrorCode.SOURCE_NOT_FOUND,
                    "source '" + sourceId + "' is not registered");
        }
        if (!e.config().isEnabled()) {
            throw new IngestionException(ErrorCode.SOURCE_DISABLED,
                    "source '" + sourceId + "' is disabled");
        }
        return e.connector();
    }

    public synchronized boolean exists(String sourceId) {
        return entries.containsKey(sourceId);
    }

    public synchronized List<SourceConfig> all() {
        return entries.values().stream().map(Entry::config).toList();
    }

    private void save() {
        Registrations r = new Registrations();
        r.sources = new ArrayList<>(entries.values().stream().map(Entry::config).toList());
        com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles.writeAtomically(persistFile, r);
    }

    public static class Registrations {
        public List<SourceConfig> sources = new ArrayList<>();
    }

    /** 隔离读取依赖，避免与 store 包形成奇怪耦合的小包装。 */
    private static final class JsonFilesReader {
        static Registrations read(Path f) {
            return com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles.read(f, Registrations.class);
        }
    }
}
