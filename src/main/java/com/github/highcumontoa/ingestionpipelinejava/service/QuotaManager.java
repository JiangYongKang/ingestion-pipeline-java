package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.model.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 运行期配额治理：每个来源的生效配额（每秒速率 / 在途上限 / 单批规模）
 * 在不重启进程的前提下可查询、可调整。
 * <ul>
 *   <li>启动时以装配配置为初始生效值，并把所有来源（推送+拉取）统一注册到
 *       RateLimiter 与 CapacityGovernor —— 任何来源都不会绕过总容量；</li>
 *   <li>调整只对此后的处理生效：已在途/已接收的数据不受影响，
 *       不丢失、不重复、不改变位点；</li>
 *   <li>非法配额（负数、在途/批规模 &lt; 1）整体拒绝，当前生效配置保持不变。</li>
 * </ul>
 */
@Component
public class QuotaManager {

    private static final Logger log = LoggerFactory.getLogger(QuotaManager.class);

    /** 某来源当前生效的配额（不可变快照）。 */
    public record Quota(int rateLimitPerSec, int maxInflight, int batchSize) {
    }

    private final SourceRegistry registry;
    private final RateLimiter rateLimiter;
    private final Map<String, Quota> effective = new ConcurrentHashMap<>();

    public QuotaManager(SourceRegistry registry, RateLimiter rateLimiter,
                        CapacityGovernor governor) {
        this.registry = registry;
        this.rateLimiter = rateLimiter;
        for (SourceDefinition def : registry.all()) {
            effective.put(def.name(),
                    new Quota(def.rateLimitPerSec(), def.maxInflight(), def.batchSize()));
            rateLimiter.register(def.name(), def.maxInflight(), def.rateLimitPerSec());
            governor.registerSource(def.name());
        }
    }

    /** 当前生效配额；未知来源抛 UNKNOWN_SOURCE。 */
    public Quota effective(String source) {
        registry.require(source);
        return effective.get(source);
    }

    /**
     * 热更新配额；为 null 的字段保持当前值。
     * 校验与生效在同一个临界区完成：非法即整体拒绝且当前配置不变。
     *
     * @return 更新后的生效配额
     */
    public synchronized Quota update(String source, Integer rateLimitPerSec,
                                     Integer maxInflight, Integer batchSize) {
        registry.require(source);
        Quota cur = effective.get(source);
        int rate = rateLimitPerSec == null ? cur.rateLimitPerSec() : rateLimitPerSec;
        int inflight = maxInflight == null ? cur.maxInflight() : maxInflight;
        int batch = batchSize == null ? cur.batchSize() : batchSize;
        if (rate < 0) {
            throw new IngestionException(RejectReason.INVALID_QUOTA,
                    "rateLimitPerSec must be >= 0 (0 = unlimited), got " + rate
                            + "; current quota unchanged");
        }
        if (inflight < 1) {
            throw new IngestionException(RejectReason.INVALID_QUOTA,
                    "maxInflight must be >= 1, got " + inflight
                            + "; current quota unchanged");
        }
        if (batch < 1) {
            throw new IngestionException(RejectReason.INVALID_QUOTA,
                    "batchSize must be >= 1, got " + batch
                            + "; current quota unchanged");
        }
        Quota next = new Quota(rate, inflight, batch);
        effective.put(source, next);
        rateLimiter.reconfigure(source, inflight, rate);
        log.info("quota updated source={} rateLimitPerSec={} maxInflight={} batchSize={}",
                source, rate, inflight, batch);
        return next;
    }
}
