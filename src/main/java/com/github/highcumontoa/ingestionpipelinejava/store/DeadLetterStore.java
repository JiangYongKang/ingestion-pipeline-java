package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.DeadLetter;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.springframework.stereotype.Component;

/**
 * 失败记录隔离区（每来源 dlq.json，原子整体落盘）。
 * 保留：原始数据、原始错误信息、归类原因、尝试次数、状态流转与时间戳。
 * 重试有界（由 maxAttempts 控制），不会无限重投。
 */
@Component
public class DeadLetterStore {

    private final Path root;
    private final Map<String, List<DeadLetter>> cache = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public DeadLetterStore(PipelineProperties props) {
        this.root = props.getStateDir().resolve("sources");
    }

    private Object lock(String source) {
        return locks.computeIfAbsent(source, k -> new Object());
    }

    private Path file(String source) {
        return root.resolve(source).resolve("dlq.json");
    }

    private List<DeadLetter> load(String source) {
        synchronized (lock(source)) {
            List<DeadLetter> list = cache.get(source);
            if (list != null) {
                return list;
            }
            List<DeadLetter> loaded = JsonFiles.read(file(source),
                    new TypeReference<List<DeadLetter>>() {
                    });
            list = loaded != null ? loaded : new ArrayList<>();
            cache.put(source, list);
            return list;
        }
    }

    private void persist(String source, List<DeadLetter> list) {
        JsonFiles.writeAtomically(file(source), list);
    }

    private static String now() {
        return Instant.now().toString();
    }

    private static boolean sameRecord(DeadLetter d, Long sequence, String key) {
        if (sequence != null && d.getSequence() != null) {
            return sequence.equals(d.getSequence());
        }
        return key != null && key.equals(d.getKey());
    }

    /** 新增或累加同一条失败记录的尝试次数；达到 attempts 上限标记 EXHAUSTED。 */
    public DeadLetter add(String source, Long sequence, IncomingRecord record,
                          RejectReason reason, String detail, int attempts) {
        synchronized (lock(source)) {
            List<DeadLetter> list = load(source);
            DeadLetter existing = list.stream()
                    .filter(d -> sameRecord(d, sequence, record == null ? null : record.key()))
                    .findFirst().orElse(null);
            DeadLetter target;
            if (existing != null) {
                target = existing;
                target.setAttempts(attempts);
            } else {
                target = new DeadLetter();
                target.setSource(source);
                target.setSequence(sequence);
                if (record != null) {
                    target.setKey(record.key());
                    target.setData(record.data());
                }
                target.setAttempts(attempts);
                target.setFirstFailedAt(now());
                list.add(target);
            }
            target.setReason(reason);
            target.setErrorDetail(detail);
            target.setLastUpdatedAt(now());
            // attempts 已达到允许上限则不再自动重投
            target.setStatus(DeadLetter.Status.PENDING);
            persist(source, list);
            return target;
        }
    }

    public List<DeadLetter> pending(String source) {
        synchronized (lock(source)) {
            return load(source).stream()
                    .filter(d -> d.getStatus() == DeadLetter.Status.PENDING)
                    .toList();
        }
    }

    public List<DeadLetter> all(String source) {
        synchronized (lock(source)) {
            return List.copyOf(load(source));
        }
    }

    public long countByStatus(String source, DeadLetter.Status status) {
        synchronized (lock(source)) {
            return load(source).stream().filter(d -> d.getStatus() == status).count();
        }
    }

    /** 原子更新某条记录状态（谓词定位）。 */
    public void update(String source, Predicate<DeadLetter> match, java.util.function.Consumer<DeadLetter> mutator) {
        synchronized (lock(source)) {
            List<DeadLetter> list = load(source);
            for (DeadLetter d : list) {
                if (match.test(d)) {
                    mutator.accept(d);
                    d.setLastUpdatedAt(now());
                }
            }
            persist(source, list);
        }
    }

    public void evict(String source) {
        synchronized (lock(source)) {
            cache.remove(source);
        }
    }
}
