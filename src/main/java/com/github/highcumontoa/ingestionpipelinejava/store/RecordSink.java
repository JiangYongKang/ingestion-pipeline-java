package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * 幂等数据落点（每来源 data.json，key -> StoredRecord）。
 * <ul>
 *   <li>键不存在 -> ACCEPTED，写入；</li>
 *   <li>键存在且内容哈希一致 -> DUPLICATE，不重复写；</li>
 *   <li>键存在但内容不一致 -> CONFLICT，保留原值（不覆盖、不丢弃），单独标记。</li>
 * </ul>
 * 每来源串行化，写入原子落盘；比较使用按键排序的规范化 JSON，判定可复现。
 */
@Component
public class RecordSink {

    private final Path root;
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, LinkedHashMap<String, StoredRecord>> cache = new ConcurrentHashMap<>();

    public RecordSink(PipelineProperties props) {
        this.root = props.getStateDir().resolve("sources");
    }

    /** 已落盘记录：内容哈希 + 结构版本。 */
    public record StoredRecord(String key, Map<String, Object> data,
                               String contentHash, int schemaVersion) {
    }

    private Object lock(String source) {
        return locks.computeIfAbsent(source, k -> new Object());
    }

    private Path file(String source) {
        return root.resolve(source).resolve("data.json");
    }

    private LinkedHashMap<String, StoredRecord> load(String source) {
        synchronized (lock(source)) {
            LinkedHashMap<String, StoredRecord> m = cache.get(source);
            if (m != null) {
                return m;
            }
            LinkedHashMap<String, StoredRecord> loaded = JsonFiles.read(file(source),
                    new TypeReference<LinkedHashMap<String, StoredRecord>>() {
                    });
            m = loaded != null ? loaded : new LinkedHashMap<>();
            cache.put(source, m);
            return m;
        }
    }

    /** 规范化内容哈希：按键名排序后做 SHA-256，结构与投递批次无关、可重复。 */
    public static String contentHash(Map<String, Object> data) {
        try {
            var canon = JsonFiles.mapper()
                    .copy()
                    .configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .writeValueAsString(data == null ? Map.of() : new TreeMap<>(data));
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canon.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("content hash failed", e);
        }
    }

    public RecordResult put(String source, IncomingRecord record, int schemaVersion) {
        synchronized (lock(source)) {
            LinkedHashMap<String, StoredRecord> data = load(source);
            String hash = contentHash(record.data());
            StoredRecord existing = data.get(record.key());
            if (existing == null) {
                data.put(record.key(), new StoredRecord(record.key(), record.data(), hash, schemaVersion));
                JsonFiles.writeAtomically(file(source), data);
                return RecordResult.accepted(record.key());
            }
            if (existing.contentHash().equals(hash)) {
                return RecordResult.duplicate(record.key());
            }
            return RecordResult.conflict(record.key(),
                    "business key '" + record.key() + "' carries different content; "
                            + "stored hash=" + existing.contentHash()
                            + ", incoming hash=" + hash);
        }
    }

    public Map<String, StoredRecord> all(String source) {
        synchronized (lock(source)) {
            return Map.copyOf(load(source));
        }
    }

    /** 测试辅助：清空内存缓存（磁盘文件另行清理）。 */
    public void evict(String source) {
        synchronized (lock(source)) {
            cache.remove(source);
        }
    }
}
