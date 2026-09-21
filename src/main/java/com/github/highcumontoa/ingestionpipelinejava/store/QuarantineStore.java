package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.error.RecordFailure;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 失败隔离区（死信队列）。
 *
 * <p>单条坏数据不中断整批：失败记录保留原始 JSON、错误码（归类）与可读原因、
 * 首次/末次失败时间与尝试次数，按业务键去重落盘为 {@code quarantine/<sourceId>.json}。
 * 重试按 attempts 上限受控：达到上限后 exhausted=true，不再被自动重投。
 */
public class QuarantineStore {

    private final Path root;
    private final Map<String, List<RecordFailure>> cache = new ConcurrentHashMap<>();

    public QuarantineStore(Path dataDir) {
        this.root = dataDir.resolve("quarantine");
    }

    private Path file(String sourceId) {
        return root.resolve(sourceId + ".json");
    }

    public List<RecordFailure> list(String sourceId) {
        return new ArrayList<>(load(sourceId));
    }

    /** 记录一次失败：同键失败累加 attempts 并更新原因；不同键分别保留。 */
    public RecordFailure recordFailure(String sourceId, String businessKey, long offset,
                                       String rawJson, ErrorCode code, String reason, int maxAttempts) {
        List<RecordFailure> all = load(sourceId);
        synchronized (all) {
            RecordFailure f = find(all, sourceId, businessKey, offset);
            long now = System.currentTimeMillis();
            if (f == null) {
                f = new RecordFailure();
                f.setSourceId(sourceId);
                f.setOffset(offset);
                f.setBusinessKey(businessKey);
                f.setRawJson(rawJson);
                f.setAttempts(0);
                f.setFirstFailedAt(now);
                all.add(f);
            }
            f.setCode(code);
            f.setReason(reason);
            f.setLastFailedAt(now);
            f.setAttempts(f.getAttempts() + 1);
            f.setExhausted(f.getAttempts() >= maxAttempts);
            persist(sourceId, all);
            return f;
        }
    }

    /** 重试成功后移除隔离记录。 */
    public void remove(String sourceId, String businessKey) {
        List<RecordFailure> all = load(sourceId);
        synchronized (all) {
            all.removeIf(f -> businessKey.equals(f.getBusinessKey()));
            persist(sourceId, all);
        }
    }

    /** 更新隔离记录保存的原始内容（运维修复死信载荷后调用）。 */
    public void updateRaw(String sourceId, String businessKey, String rawJson) {
        List<RecordFailure> all = load(sourceId);
        synchronized (all) {
            for (RecordFailure f : all) {
                if (businessKey.equals(f.getBusinessKey())) {
                    f.setRawJson(rawJson);
                    persist(sourceId, all);
                    return;
                }
            }
        }
    }

    /** 未耗尽重试次数的记录才允许再次投递，避免无限重投。 */
    public List<RecordFailure> retryable(String sourceId) {
        return load(sourceId).stream().filter(f -> !f.isExhausted()).toList();
    }

    private List<RecordFailure> load(String sourceId) {
        return cache.computeIfAbsent(sourceId, id -> {
            Path f = file(id);
            if (!Files.exists(f)) {
                List<RecordFailure> l = new ArrayList<>();
                persist(id, l);
                return l;
            }
            return new ArrayList<>(JsonFiles.read(f, Failures.class).records);
        });
    }

    private void persist(String sourceId, List<RecordFailure> all) {
        Failures wrapper = new Failures();
        wrapper.records = new ArrayList<>(all);
        Path target = file(sourceId);
        try {
            Files.createDirectories(root);
            Path tmp = Files.createTempFile(root, ".q-", ".json");
            Files.writeString(tmp, new String(JsonFiles.toBytes(wrapper), StandardCharsets.UTF_8));
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist quarantine for " + sourceId, e);
        }
    }

    private static RecordFailure find(List<RecordFailure> all, String sourceId,
                                      String businessKey, long offset) {
        if (businessKey != null) {
            for (RecordFailure f : all) {
                if (businessKey.equals(f.getBusinessKey())) {
                    return f;
                }
            }
        }
        for (RecordFailure f : all) {
            if (f.getOffset() == offset && sourceId.equals(f.getSourceId())) {
                return f;
            }
        }
        return null;
    }

    public static class Failures {
        public List<RecordFailure> records = new ArrayList<>();
    }
}
