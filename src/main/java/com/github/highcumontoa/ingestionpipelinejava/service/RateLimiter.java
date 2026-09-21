package com.github.highcumontoa.ingestionpipelinejava.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * 每来源有界积压与限速（状态总量 = 来源数，内存有界）：
 * <ul>
 *   <li>在途计数超过 maxInflight -> 拒绝（BACKLOG_FULL），绝不无限排队；</li>
 *   <li>令牌桶（容量=每秒配额，按经过时间补充）不足 -> 拒绝（RATE_LIMITED）。</li>
 * </ul>
 */
@Component
public class RateLimiter {

    private static final class Bucket {
        final int maxInflight;
        final int ratePerSec;
        int inflight;
        double tokens;
        long lastRefillNanos;

        Bucket(int maxInflight, int ratePerSec) {
            this.maxInflight = maxInflight;
            this.ratePerSec = ratePerSec;
            this.tokens = Math.max(ratePerSec, 0);
            this.lastRefillNanos = System.nanoTime();
        }
    }

    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public void register(String source, int maxInflight, int ratePerSec) {
        buckets.compute(source, (k, old) -> old == null
                ? new Bucket(maxInflight, ratePerSec)
                : new Bucket(maxInflight, ratePerSec));
    }

    /** 获取一个在途许可，满则 false（由调用方归为 BACKLOG_FULL）。 */
    public boolean tryAcquireInflight(String source) {
        Bucket b = buckets.get(source);
        if (b == null) {
            return true;
        }
        synchronized (b) {
            if (b.inflight >= b.maxInflight) {
                return false;
            }
            b.inflight++;
            return true;
        }
    }

    public void releaseInflight(String source) {
        Bucket b = buckets.get(source);
        if (b == null) {
            return;
        }
        synchronized (b) {
            if (b.inflight > 0) {
                b.inflight--;
            }
        }
    }

    public int inflight(String source) {
        Bucket b = buckets.get(source);
        return b == null ? 0 : b.inflight;
    }

    /** 尝试为 n 条记录取令牌；ratePerSec&lt;=0 表示不限速。 */
    public boolean tryAcquireToken(String source, int records) {
        Bucket b = buckets.get(source);
        if (b == null || b.ratePerSec <= 0) {
            return true;
        }
        synchronized (b) {
            long now = System.nanoTime();
            double elapsedSec = (now - b.lastRefillNanos) / 1_000_000_000.0;
            b.tokens = Math.min(b.ratePerSec, b.tokens + elapsedSec * b.ratePerSec);
            b.lastRefillNanos = now;
            if (b.tokens >= records) {
                b.tokens -= records;
                return true;
            }
            return false;
        }
    }
}
