package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.model.SourceMetrics;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每来源处理计数的持久化存储。接收/写入/重复/冲突/隔离/拒绝/重试均可查询，
 * 重启后延续计数，保证结果可观测、可复现。
 */
public class MetricsStore {

    private final Path root;
    private final Map<String, SourceMetrics> cache = new ConcurrentHashMap<>();

    public MetricsStore(Path dataDir) {
        this.root = dataDir.resolve("metrics");
    }

    private Path file(String sourceId) {
        return root.resolve(sourceId + ".json");
    }

    public SourceMetrics get(String sourceId) {
        return cache.computeIfAbsent(sourceId, id -> {
            Path f = file(id);
            if (Files.exists(f)) {
                return JsonFiles.read(f, SourceMetrics.class);
            }
            SourceMetrics m = new SourceMetrics(id);
            persist(m);
            return m;
        });
    }

    public List<SourceMetrics> all() {
        List<SourceMetrics> out = new ArrayList<>(cache.values());
        if (out.isEmpty()) {
            try (var paths = Files.list(root)) {
                paths.filter(p -> p.toString().endsWith(".json")).forEach(p ->
                        out.add(JsonFiles.read(p, SourceMetrics.class)));
            } catch (Exception ignore) {
                // 目录尚不存在时返回空集合
            }
        }
        return out;
    }

    public synchronized void increment(String sourceId, java.util.function.Consumer<SourceMetrics> change) {
        SourceMetrics m = get(sourceId);
        synchronized (m) {
            change.accept(m);
            persist(m);
        }
    }

    private void persist(SourceMetrics m) {
        JsonFiles.writeAtomically(file(m.getSourceId()), m);
    }
}
