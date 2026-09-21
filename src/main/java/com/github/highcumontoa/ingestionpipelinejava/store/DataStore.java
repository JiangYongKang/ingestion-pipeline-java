package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.model.WriteOutcome;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 每来源的幂等数据存储。
 *
 * <p>落盘布局（{@code data/<sourceId>/}）：
 * <ul>
 *   <li>{@code data.jsonl}：追加式 WAL，每行一个 INSERTED 事件（JSON）；</li>
 *   <li>{@code snapshot.json}：压缩后的当前键值视图，崩溃恢复时快照 + WAL 重放；</li>
 *   <li>{@code conflicts.jsonl}：同键不同内容的冲突证据，只追加。</li>
 * </ul>
 * 判定规则：业务键相同 + 规范化内容哈希相同 ⇒ DUPLICATE（幂等）；
 * 业务键相同但哈希不同 ⇒ CONFLICT，保留双方且不覆盖；键不存在 ⇒ INSERTED。
 *
 * <p>本类自身方法以来源为粒度串行化；与位点的联合原子性由 IngestionService
 * 在 OffsetStore 的来源锁内编排。
 */
public class DataStore {

    private final Path dataDir;
    private final Map<String, SourceState> states = new LinkedHashMap<>();

    public DataStore(Path dataDir) {
        this.dataDir = dataDir.resolve("data");
    }

    private static final class SourceState {
        final Map<String, StoredRecord> byKey = new LinkedHashMap<>();
        /** 已记录冲突的去重指纹（业务键 + 传入哈希 + 既有哈希），保证重放不重复记账。 */
        final java.util.Set<String> conflictSeen = new java.util.HashSet<>();
        long lastCompaction;
    }

    private SourceState state(String sourceId) {
        SourceState s = states.get(sourceId);
        if (s != null) {
            return s;
        }
        synchronized (this) {
            SourceState existing = states.get(sourceId);
            if (existing != null) {
                return existing;
            }
            SourceState created = new SourceState();
            load(sourceId, created);
            states.put(sourceId, created);
            return created;
        }
    }

    private void load(String sourceId, SourceState s) {
        Path dir = sourceDir(sourceId);
        Path snapshot = dir.resolve("snapshot.json");
        if (Files.exists(snapshot)) {
            Snapshot snap = JsonFiles.read(snapshot, Snapshot.class);
            s.lastCompaction = snap.lastOffset;
            if (snap.records != null) {
                for (StoredRecord r : snap.records) {
                    s.byKey.put(r.getBusinessKey(), r);
                }
            }
        }
        Path wal = dir.resolve("data.jsonl");
        if (Files.exists(wal)) {
            try {
                for (String line : Files.readAllLines(wal, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    StoredRecord r = JsonFiles.mapper().readValue(line, StoredRecord.class);
                    if (r.getFirstOffset() > s.lastCompaction) {
                        s.byKey.put(r.getBusinessKey(), r);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed to replay wal for " + sourceId, e);
            }
        }
        Path conflicts = dir.resolve("conflicts.jsonl");
        if (Files.exists(conflicts)) {
            try {
                for (String line : Files.readAllLines(conflicts, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    ConflictRecord c = JsonFiles.mapper().readValue(line, ConflictRecord.class);
                    s.conflictSeen.add(c.getBusinessKey() + "|" + c.getIncomingHash()
                            + "|" + c.getStoredHash());
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed to replay conflicts for " + sourceId, e);
            }
        }
    }

    /**
     * 幂等写入。返回 INSERTED / DUPLICATE / CONFLICT；
     * 冲突时将传入版本追加到 conflicts.jsonl，既有数据保持不变。
     */
    public WriteOutcome write(String sourceId, String businessKey, JsonNode canonicalPayload, long offset) {
        SourceState s = state(sourceId);
        synchronized (s) {
            String hash = hashOf(canonical(canonicalPayload));
            StoredRecord existing = s.byKey.get(businessKey);
            if (existing == null) {
                StoredRecord rec = new StoredRecord();
                rec.setBusinessKey(businessKey);
                rec.setPayload(canonicalPayload);
                rec.setContentHash(hash);
                rec.setFirstOffset(offset);
                rec.setWrittenAt(System.currentTimeMillis());
                appendWal(sourceDir(sourceId).resolve("data.jsonl"), rec);
                s.byKey.put(businessKey, rec);
                return WriteOutcome.INSERTED;
            }
            if (existing.getContentHash().equals(hash)) {
                return WriteOutcome.DUPLICATE;
            }
            ConflictRecord c = new ConflictRecord();
            c.setBusinessKey(businessKey);
            c.setIncomingPayload(canonicalPayload);
            c.setIncomingHash(hash);
            c.setStoredHash(existing.getContentHash());
            c.setIncomingOffset(offset);
            c.setDetectedAt(System.currentTimeMillis());
            String fingerprint = businessKey + "|" + hash + "|" + existing.getContentHash();
            if (!s.conflictSeen.add(fingerprint)) {
                return WriteOutcome.CONFLICT;
            }
            appendLine(sourceDir(sourceId).resolve("conflicts.jsonl"), toJson(c));
            return WriteOutcome.CONFLICT;
        }
    }

    /** 将 WAL 压缩为快照，并截断已覆盖的日志（原子替换文件）。 */
    public void compact(String sourceId, long durableOffset) {
        SourceState s = state(sourceId);
        synchronized (s) {
            Snapshot snap = new Snapshot();
            snap.lastOffset = durableOffset;
            snap.records = new ArrayList<>(s.byKey.values());
            Path dir = sourceDir(sourceId);
            JsonFiles.writeAtomically(dir.resolve("snapshot.json"), snap);
            Path wal = dir.resolve("data.jsonl");
            try {
                Files.write(wal, new byte[0], StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to truncate wal for " + sourceId, e);
            }
            s.lastCompaction = durableOffset;
        }
    }

    public List<StoredRecord> all(String sourceId) {
        SourceState s = state(sourceId);
        synchronized (s) {
            return new ArrayList<>(s.byKey.values());
        }
    }

    public StoredRecord get(String sourceId, String businessKey) {
        SourceState s = state(sourceId);
        synchronized (s) {
            return s.byKey.get(businessKey);
        }
    }

    public List<ConflictRecord> conflicts(String sourceId) {
        Path f = sourceDir(sourceId).resolve("conflicts.jsonl");
        List<ConflictRecord> out = new ArrayList<>();
        if (!Files.exists(f)) {
            return out;
        }
        try {
            for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    out.add(JsonFiles.mapper().readValue(line, ConflictRecord.class));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read conflicts for " + sourceId, e);
        }
        return out;
    }

    private Path sourceDir(String sourceId) {
        Path dir = dataDir.resolve(sourceId);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return dir;
    }

    private static void appendWal(Path file, StoredRecord rec) {
        appendLine(file, toJson(rec));
    }

    private static void appendLine(Path file, String json) {
        try {
            Files.writeString(file, json + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append " + file, e);
        }
    }

    private static String toJson(Object value) {
        try {
            return JsonFiles.mapper().writeValueAsString(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 规范化：对象字段按 key 排序后序列化，保证字段顺序不同但内容相同的投递
     * 被判定为 DUPLICATE 而不是误报冲突。
     */
    @SuppressWarnings("unchecked")
    private static JsonNode canonical(JsonNode node) {
        if (node == null || !node.isObject()) {
            return node;
        }
        TreeMap<String, Object> sorted = new TreeMap<>();
        node.fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            if (v.isObject()) {
                sorted.put(e.getKey(), JsonFiles.mapper().convertValue(canonical(v), JsonNode.class));
            } else {
                sorted.put(e.getKey(), JsonFiles.mapper().convertValue(v, JsonNode.class));
            }
        });
        return JsonFiles.mapper().valueToTree(sorted);
    }

    private static String hashOf(JsonNode node) {
        try {
            return sha256(JsonFiles.mapper().writeValueAsString(node));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 快照文件结构。 */
    public static class Snapshot {
        public long lastOffset;
        public List<StoredRecord> records;
    }
}
