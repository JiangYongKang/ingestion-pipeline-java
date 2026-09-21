package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * 结构版本注册表（每来源 schema.json，按版本保存全部历史）。
 * <ul>
 *   <li>结构只能顺序演进：首版必须为 1，之后声明版本必须恰好 = 当前版本 + 1；</li>
 *   <li>新增字段必须是非必填（默认 null，语义确定且稳定）；不允许重新加入已删除列；</li>
 *   <li>被删除字段记入 dropped 名单并跨版本保留，数据中再出现即 COLUMN_DROPPED。</li>
 * </ul>
 */
@Component
public class SchemaStore {

    /** 落盘格式：活跃字段 + 历史累计已删除字段。 */
    public record StoredSchema(int version, List<SchemaField> fields, List<String> dropped) {
    }

    private final Path root;
    private final Map<String, List<StoredSchema>> cache = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public SchemaStore(PipelineProperties props) {
        this.root = props.getStateDir().resolve("sources");
    }

    private Object lock(String source) {
        return locks.computeIfAbsent(source, k -> new Object());
    }

    private Path file(String source) {
        return root.resolve(source).resolve("schema.json");
    }

    private List<StoredSchema> load(String source) {
        synchronized (lock(source)) {
            List<StoredSchema> list = cache.get(source);
            if (list != null) {
                return list;
            }
            List<StoredSchema> loaded = JsonFiles.read(file(source),
                    new TypeReference<List<StoredSchema>>() {
                    });
            list = loaded != null ? loaded : new ArrayList<>();
            cache.put(source, list);
            return list;
        }
    }

    public boolean exists(String source) {
        return !load(source).isEmpty();
    }

    public RecordSchema latest(String source) {
        List<StoredSchema> all = load(source);
        if (all.isEmpty()) {
            return null;
        }
        StoredSchema s = all.get(all.size() - 1);
        return new RecordSchema(s.version(), s.fields());
    }

    /** 截至当前版本累计被删除的列名。 */
    public Set<String> droppedFields(String source) {
        List<StoredSchema> all = load(source);
        if (all.isEmpty()) {
            return Set.of();
        }
        return new LinkedHashSet<>(all.get(all.size() - 1).dropped());
    }

    public RecordSchema register(String source, RecordSchema declared) {
        if (declared == null || declared.fields() == null) {
            throw new IngestionException(RejectReason.SCHEMA_VERSION_INVALID,
                    "schema declaration is empty");
        }
        checkUniqueNames(source, declared);
        synchronized (lock(source)) {
            List<StoredSchema> history = load(source);
            int expectedVersion = history.isEmpty() ? 1 : history.get(history.size() - 1).version() + 1;
            if (declared.version() != expectedVersion) {
                throw new IngestionException(RejectReason.SCHEMA_VERSION_INVALID,
                        "source '" + source + "' schema version must be " + expectedVersion
                                + " (sequential +1) but got " + declared.version());
            }
            List<String> dropped;
            if (history.isEmpty()) {
                dropped = List.of();
            } else {
                StoredSchema prev = history.get(history.size() - 1);
                Set<String> prevNames = prev.fields().stream()
                        .map(SchemaField::name).collect(Collectors.toCollection(LinkedHashSet::new));
                Set<String> newNames = declared.fields().stream()
                        .map(SchemaField::name).collect(Collectors.toSet());
                Set<String> newlyDropped = new LinkedHashSet<>(prevNames);
                newlyDropped.removeAll(newNames);

                Set<String> prevDropped = new LinkedHashSet<>(prev.dropped());
                // 已删除列不允许重新加入：避免默认值语义二义
                for (SchemaField f : declared.fields()) {
                    if (prevDropped.contains(f.name())) {
                        throw new IngestionException(RejectReason.SCHEMA_VERSION_INVALID,
                                "source '" + source + "' field '" + f.name()
                                        + "' was dropped and cannot be re-added; choose a new field name");
                    }
                }
                // 新增字段必须可选（默认 null）
                for (SchemaField f : declared.fields()) {
                    if (!prevNames.contains(f.name()) && f.required()) {
                        throw new IngestionException(RejectReason.SCHEMA_VERSION_INVALID,
                                "source '" + source + "' newly added field '" + f.name()
                                        + "' must be optional (deterministic default: null)");
                    }
                }
                dropped = new ArrayList<>(prevDropped);
                dropped.addAll(newlyDropped);
            }
            StoredSchema stored = new StoredSchema(declared.version(), declared.fields(), dropped);
            List<StoredSchema> next = new ArrayList<>(history);
            next.add(stored);
            JsonFiles.writeAtomically(file(source), next);
            cache.put(source, next);
            return new RecordSchema(stored.version(), stored.fields());
        }
    }

    private void checkUniqueNames(String source, RecordSchema schema) {
        Set<String> seen = new HashSet<>();
        for (SchemaField f : schema.fields()) {
            if (f.name() == null || f.name().isBlank()) {
                throw new IngestionException(RejectReason.SCHEMA_VERSION_INVALID,
                        "source '" + source + "' schema contains blank field name");
            }
            if (!seen.add(f.name())) {
                throw new IngestionException(RejectReason.SCHEMA_VERSION_INVALID,
                        "source '" + source + "' schema field duplicated: " + f.name());
            }
        }
    }

    public void evict(String source) {
        synchronized (lock(source)) {
            cache.remove(source);
        }
    }
}
