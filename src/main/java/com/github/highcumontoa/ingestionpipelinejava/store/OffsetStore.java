package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * 位点持久化。每来源一个单调游标，落盘（原子写）保证重启恢复。
 * <ul>
 *   <li>回退（next &lt; 已提交 或 expected 不等于已提交）-> OFFSET_BACKWARD；</li>
 *   <li>拉取模式非连续推进 -> OFFSET_GAP；</li>
 *   <li>每来源一把锁串行提交，并发推进不跳变、不越位、不错位。</li>
 * </ul>
 */
@Component
public class OffsetStore {

    private final Path root;
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, Long> cached = new ConcurrentHashMap<>();

    public OffsetStore(PipelineProperties props) {
        this.root = props.getStateDir().resolve("sources");
    }

    private Object lock(String source) {
        return locks.computeIfAbsent(source, k -> new Object());
    }

    private Path file(String source) {
        return root.resolve(source).resolve("offset.json");
    }

    /** 当前已确认位点（未提交过为 0）。 */
    public long current(String source) {
        Long v = cached.get(source);
        if (v != null) {
            return v;
        }
        synchronized (lock(source)) {
            Map<String, Long> data = JsonFiles.read(file(source),
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Long>>() {
                    });
            long cur = data == null || data.get("offset") == null ? 0L : data.get("offset");
            cached.put(source, cur);
            return cur;
        }
    }

    /**
     * 提交位点。
     *
     * @param expected         调用方观察到的当前位点；必须与已提交值一致
     * @param next             新位点；必须不小于当前值
     * @param strictContiguous true 时 next 必须恰为 当前值+1（拉取模式）
     */
    public void commitContiguous(String source, long expected, long next, boolean strictContiguous) {
        synchronized (lock(source)) {
            long cur = current(source);
            if (expected != cur) {
                throw new IngestionException(RejectReason.OFFSET_BACKWARD,
                        "source '" + source + "' cursor changed concurrently: expected "
                                + expected + " but committed is " + cur);
            }
            if (next < cur) {
                throw new IngestionException(RejectReason.OFFSET_BACKWARD,
                        "source '" + source + "' offset must not go backward: " + next + " < " + cur);
            }
            if (strictContiguous && next != cur && next != cur + 1) {
                throw new IngestionException(RejectReason.OFFSET_GAP,
                        "source '" + source + "' non-contiguous offset: " + cur + " -> " + next);
            }
            if (next == cur) {
                return; // 幂等提交
            }
            JsonFiles.writeAtomically(file(source), Map.of("offset", next));
            cached.put(source, next);
        }
    }
}
