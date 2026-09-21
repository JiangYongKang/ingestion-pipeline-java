package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 位点持久化与重启恢复。
 *
 * <p>每个来源一把锁，推进必须满足：
 * <ol>
 *   <li>expectedVersion 与当前版本一致（拒绝并发下的陈旧提交，防跳变/越位）；</li>
 *   <li>newOffset 大于当前位点（拒绝回退）；</li>
 * </ol>
 * 连续性（必须紧邻）由调用方在持锁区间内判定，保证“数据写入 + 位点推进”原子。
 */
public class OffsetStore {

    private final Path root;
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public OffsetStore(Path dataDir) {
        this.root = dataDir.resolve("offsets");
    }

    public Object lock(String sourceId) {
        return locks.computeIfAbsent(sourceId, k -> new Object());
    }

    private Path file(String sourceId) {
        return root.resolve(sourceId + ".json");
    }

    /** 读取已确认位点；首次访问的来源从 0 开始。 */
    public OffsetState load(String sourceId) {
        Path f = file(sourceId);
        if (!Files.exists(f)) {
            OffsetState fresh = new OffsetState();
            JsonFiles.writeAtomically(f, fresh);
            return fresh;
        }
        return JsonFiles.read(f, OffsetState.class);
    }

    /**
     * CAS 式推进位点。任何拒绝都不会改变落盘状态。
     *
     * @param expectedVersion 调用方读到的版本；与当前不一致则拒绝
     * @param newOffset       新位点，必须严格大于当前位点
     * @return 推进后的新状态
     */
    public OffsetState advance(String sourceId, long expectedVersion, long newOffset) {
        synchronized (lock(sourceId)) {
            OffsetState state = load(sourceId);
            if (state.getVersion() != expectedVersion) {
                throw new IngestionException(ErrorCode.OFFSET_VERSION_STALE,
                        "stale offset version for source '" + sourceId + "': expected "
                                + state.getVersion() + " but submitted " + expectedVersion);
            }
            if (newOffset < state.getOffset()) {
                throw new IngestionException(ErrorCode.OFFSET_FALLBACK,
                        "offset fallback refused for source '" + sourceId + "': current "
                                + state.getOffset() + ", submitted " + newOffset);
            }
            if (newOffset == state.getOffset() && newOffset != 0) {
                throw new IngestionException(ErrorCode.OFFSET_NON_MONOTONIC,
                        "offset must strictly advance for source '" + sourceId + "': " + newOffset);
            }
            state.setOffset(newOffset);
            state.setVersion(state.getVersion() + 1);
            JsonFiles.writeAtomically(file(sourceId), state);
            return state;
        }
    }
}
