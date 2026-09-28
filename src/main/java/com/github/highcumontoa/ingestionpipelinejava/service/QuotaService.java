package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 运行期来源配额注册表：每来源生效中的 (batchSize, rateLimitPerSec, maxInflight)。
 * <ul>
 *   <li>启动时从 SourceRegistry 的定义播种：<b>推送与拉取所有来源</b>都立即拿到
 *       生效中的速率/在途/单批规模限制，进程刚启动、尚未做任何运行期调整时
 *             也不会放行连续突发（此前推送桶只在首次热调整时才创建，存在启动期穿透）；</li>
 *   <li>支持不重启热调整：新配额只对此后的处理生效，已接收/在处理的数据不受影响，
 *       不产生丢失、重复或位点错乱（在途计数保留；令牌补到新配额满桶，保证立即生效）；</li>
 *   <li>非法配额（batchSize&lt;=0、rateLimitPerSec&lt;0、maxInflight&lt;=0）被拒绝，
 *       当前生效配置保持不变；</li>
 *   <li>生效中的配额可随时查询（调整前后对比可解释）。</li>
 * </ul>
 */
@Service
public class QuotaService {

    private static final Logger log = LoggerFactory.getLogger(QuotaService.class);

    /** 单来源生效配额（不可变快照）。 */
    public record Quota(int batchSize, int rateLimitPerSec, int maxInflight) {
    }

    private final SourceRegistry registry;
    private final RateLimiter rateLimiter;
    private final Map<String, Quota> quotas = new ConcurrentHashMap<>();

    public QuotaService(SourceRegistry registry, RateLimiter rateLimiter) {
        this.registry = registry;
        this.rateLimiter = rateLimiter;
        for (SourceDefinition d : registry.all()) {
            Quota q = new Quota(d.batchSize(), d.rateLimitPerSec(), d.maxInflight());
            quotas.put(d.name(), q);
            // 关键：启动即把每个来源（含 PUSH）的限速/积压桶按启动配置就位，
            // 不依赖首次运行期调整；register 幂等，不覆盖已存在的桶。
            rateLimiter.register(d.name(), d.maxInflight(), d.rateLimitPerSec());
        }
    }

    /** 查询某来源当前生效配额。 */
    public Quota effective(String source) {
        SourceDefinition def = registry.require(source);
        Quota existing = quotas.get(def.name());
        if (existing != null) {
            return existing;
        }
        Quota seeded = new Quota(def.batchSize(), def.rateLimitPerSec(), def.maxInflight());
        Quota winner = quotas.putIfAbsent(def.name(), seeded);
        if (winner == null) {
            // 并发下首次播种也要保证限速/积压桶立即就位
            rateLimiter.register(def.name(), def.maxInflight(), def.rateLimitPerSec());
            return seeded;
        }
        return winner;
    }

    /**
     * 热调整配额；null 字段表示保持不变。
     * 非法值抛 INVALID_QUOTA 且当前生效配置不变。
     */
    public synchronized Quota update(String source, Integer batchSize, Integer rateLimitPerSec,
                                     Integer maxInflight) {
        SourceDefinition def = registry.require(source);
        Quota cur = effective(source);
        int newBatch = batchSize == null ? cur.batchSize() : batchSize;
        int newRate = rateLimitPerSec == null ? cur.rateLimitPerSec() : rateLimitPerSec;
        int newInflight = maxInflight == null ? cur.maxInflight() : maxInflight;
        if (newBatch <= 0) {
            throw new IngestionException(RejectReason.INVALID_QUOTA,
                    "source '" + source + "' batch-size must be positive, got " + newBatch
                            + "; current quota unchanged: " + cur);
        }
        if (newRate < 0) {
            throw new IngestionException(RejectReason.INVALID_QUOTA,
                    "source '" + source + "' rate-limit-per-sec must be >= 0, got " + newRate
                            + "; current quota unchanged: " + cur);
        }
        if (newInflight <= 0) {
            throw new IngestionException(RejectReason.INVALID_QUOTA,
                    "source '" + source + "' max-inflight must be positive, got " + newInflight
                            + "; current quota unchanged: " + cur);
        }
        Quota next = new Quota(newBatch, newRate, newInflight);
        quotas.put(def.name(), next);
        // 就地更新限速/积压参数：保留在途计数与令牌，只影响此后的获取
        rateLimiter.updateQuota(def.name(), newInflight, newRate);
        log.info("quota updated source={} {} -> {}", source, cur, next);
        return next;
    }
}
