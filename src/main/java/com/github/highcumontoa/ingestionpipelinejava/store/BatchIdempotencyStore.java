package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.model.BatchResult;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * 批次幂等存储：每来源每个 batchId 一份首次处理结果。
 * 整批重放时直接返回首次结果，不产生任何重复写入或位点变化。
 * 落盘保证重启后重放仍被识别。
 */
@Component
public class BatchIdempotencyStore {

    private final Path root;
    private final ConcurrentMap<String, BatchResult> cache = new ConcurrentHashMap<>();

    public BatchIdempotencyStore(PipelineProperties props) {
        this.root = props.getStateDir().resolve("sources");
    }

    private static String key(String source, String batchId) {
        return source + "/" + batchId;
    }

    private Path file(String source, String batchId) {
        // 仅允许安全字符进入文件名，避免路径穿越
        String safe = batchId.replaceAll("[^A-Za-z0-9._-]", "_");
        return root.resolve(source).resolve("batches").resolve(safe + ".json");
    }

    public BatchResult get(String source, String batchId) {
        BatchResult cachedResult = cache.get(key(source, batchId));
        if (cachedResult != null) {
            return cachedResult;
        }
        BatchResult onDisk = JsonFiles.read(file(source, batchId), BatchResult.class);
        if (onDisk != null) {
            cache.put(key(source, batchId), onDisk);
        }
        return onDisk;
    }

    public void save(String source, String batchId, BatchResult result) {
        JsonFiles.writeAtomically(file(source, batchId), result);
        cache.put(key(source, batchId), result);
    }
}
