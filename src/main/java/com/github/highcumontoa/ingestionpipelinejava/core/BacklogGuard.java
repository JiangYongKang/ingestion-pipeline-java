package com.github.highcumontoa.ingestionpipelinejava.core;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 每来源的有界积压控制：以“已准入但未处理完”的记录条数作为配额。
 *
 * <p>策略：
 * <ul>
 *   <li>REJECT（默认）：配额不足立即拒绝，错误码 BACKLOG_LIMIT_REACHED；</li>
 *   <li>RATE_LIMIT：在 {@code ingestion.backlog-await-millis} 内等待空位，
 *       超时仍不足才拒绝，形成确定性的限速效果。</li>
 * </ul>
 * 配额使并发请求不会在内存中无界堆积；未授权请求在准入前即被拒绝。
 */
public class BacklogGuard {

    private final PipelineProperties props;
    private final Map<String, Semaphore> permits = new ConcurrentHashMap<>();

    public BacklogGuard(PipelineProperties props) {
        this.props = props;
    }

    private Semaphore semaphore(String sourceId) {
        return permits.computeIfAbsent(sourceId,
                k -> new Semaphore(props.getBacklogLimit(), true));
    }

    public void acquire(String sourceId, int recordCount) {
        if (recordCount <= 0) {
            return;
        }
        int limit = props.getBacklogLimit();
        if (recordCount > limit) {
            throw new IngestionException(ErrorCode.BACKLOG_LIMIT_REACHED,
                    "batch of " + recordCount + " records exceeds backlog limit " + limit
                            + " for source '" + sourceId + "'");
        }
        Semaphore sem = semaphore(sourceId);
        boolean ok;
        if ("RATE_LIMIT".equalsIgnoreCase(props.getBacklogPolicy())) {
            try {
                ok = sem.tryAcquire(recordCount, props.getBacklogAwaitMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IngestionException(ErrorCode.BACKLOG_LIMIT_REACHED,
                        "interrupted while waiting for backlog capacity");
            }
        } else {
            ok = sem.tryAcquire(recordCount);
        }
        if (!ok) {
            throw new IngestionException(ErrorCode.BACKLOG_LIMIT_REACHED,
                    "backlog limit " + limit + " reached for source '" + sourceId
                            + "' (policy=" + props.getBacklogPolicy() + ")");
        }
    }

    public void release(String sourceId, int recordCount) {
        if (recordCount > 0) {
            semaphore(sourceId).release(recordCount);
        }
    }
}
