package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * 进程级总容量治理：所有来源（推送/拉取）共享同一份在途许可池。
 * <ul>
 *   <li>总量上限：全局在途占用达到 total 后，新到处理一律拒绝（GLOBAL_CAPACITY_EXHAUSTED），
 *       不无限排队、不撑爆内存；</li>
 *   <li>公平分配（确定、可复现）：每来源静态公平份额 share = max(1, total / N)，
 *       N 为启动时注册来源数；任何来源在全局池中的占用不得超过其份额。
 *       因为 N * share <= total（share 取下整），份额之和不超过总量，
 *       所以任何来源长期占满自己的份额也无法挤占其它来源的份额，
 *       每个来源在任何时刻都保证能拿到属于自己的份额；</li>
 *   <li>许可语义：一次推送批次 / 一轮拉取处理 = 1 个许可，与按来源 maxInflight 同维度。</li>
 * </ul>
 */
@Component
public class CapacityGovernor {

    /** 全局容量快照（可观测）。 */
    public record CapacitySnapshot(int total, int inUse, int sourceCount, int fairShare,
                                   Map<String, Integer> perSourceInUse) {
    }

    private final int total;
    private final int fairShare;
    private final int sourceCount;
    private final ConcurrentMap<String, AtomicInteger> perSource = new ConcurrentHashMap<>();
    private final AtomicInteger inUse = new AtomicInteger();

    @org.springframework.beans.factory.annotation.Autowired
    public CapacityGovernor(PipelineProperties props, SourceRegistry registry) {
        this(props.getGlobalMaxInflight(), Math.max(1, registry.all().size()));
    }

    /** 测试/独立装配用：直接给定总量与来源数。total<=0 表示不启用总上限。 */
    public CapacityGovernor(int total, int sourceCount) {
        this.total = total;
        this.sourceCount = Math.max(1, sourceCount);
        this.fairShare = total <= 0 ? Integer.MAX_VALUE
                : Math.max(1, total / this.sourceCount);
    }

    /** 获取一个全局在途许可；到顶或超出本来源公平份额则 false。 */
    public boolean tryAcquire(String source) {
        if (total <= 0) {
            return true;
        }
        AtomicInteger mine = perSource.computeIfAbsent(source, k -> new AtomicInteger());
        // 先占来源份额（确定性上限），再占全局总量；任一步失败都回滚，不留半占状态
        while (true) {
            int cur = mine.get();
            if (cur >= fairShare) {
                return false;
            }
            if (mine.compareAndSet(cur, cur + 1)) {
                break;
            }
        }
        while (true) {
            int cur = inUse.get();
            if (cur >= total) {
                mine.decrementAndGet();
                return false;
            }
            if (inUse.compareAndSet(cur, cur + 1)) {
                return true;
            }
        }
    }

    /** 释放一个全局在途许可。 */
    public void release(String source) {
        if (total <= 0) {
            return;
        }
        AtomicInteger mine = perSource.get(source);
        if (mine != null) {
            mine.updateAndGet(v -> Math.max(0, v - 1));
        }
        inUse.updateAndGet(v -> Math.max(0, v - 1));
    }

    /** 当前全局快照。 */
    public CapacitySnapshot snapshot() {
        Map<String, Integer> per = new TreeMap<>();
        perSource.forEach((k, v) -> per.put(k, v.get()));
        return new CapacitySnapshot(total, total <= 0 ? 0 : inUse.get(),
                sourceCount, total <= 0 ? 0 : fairShare, per);
    }
}
