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
        int maxInflight;
        int ratePerSec;
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

    /**
     * 启动期注册某来源的在途/限速桶。幂等：来源已注册（如已被配额服务播种、
     * 或已做过运行期热调整）时保留既有桶与运行期状态，绝不把生效配置重置回静态值。
     */
    public void register(String source, int maxInflight, int ratePerSec) {
        buckets.computeIfAbsent(source, k -> new Bucket(maxInflight, ratePerSec));
    }

    /**
     * 运行期热更新配额：就地修改参数，保留当前在途计数（已在途的处理不受影响）。
     * 令牌补到新配额的满桶：保证调大速率后新配额对此后请求立即生效，
     * 调小时也最多只放行一个新满桶的突发，随后按新速率收敛。
     */
    public synchronized void updateQuota(String source, int maxInflight, int ratePerSec) {
        Bucket b = buckets.computeIfAbsent(source, k -> new Bucket(maxInflight, ratePerSec));
        synchronized (b) {
            b.maxInflight = maxInflight;
            b.ratePerSec = ratePerSec;
            b.tokens = Math.max(ratePerSec, 0);
            b.lastRefillNanos = System.nanoTime();
        }
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
