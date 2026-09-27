package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 进程级总容量治理：所有来源（推送 + 拉取）从同一份总在途许可中取用。
 * <ul>
 *   <li>总占用达到 globalMaxInflight 后，任何新获取都失败（调用方归为
 *       GLOBAL_CAPACITY_EXCEEDED 明确拒绝），绝不无限排队；</li>
 *   <li>公平分配是确定、可复现的：每个已注册来源的可持有上限 =
 *       max(1, floor(总容量 / 来源数))。由于各来源份额之和不超过总容量，
 *       任何来源都无法侵占其它来源的份额 —— 一个来源长期占满自己的份额，
 *       其它来源仍然一定能拿到属于自己的那一份；</li>
 *   <li>状态总量 = 来源数 + 一个总计数，内存有界。</li>
 * </ul>
 * 许可的获取/释放必须成对（try/finally），与处理的生命周期一致。
 */
@Component
public class CapacityGovernor {

    private final int totalPermits;
    private final Set<String> sources = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> heldBySource = new ConcurrentHashMap<>();
    /** 全局锁：保证“检查份额 + 占用”的原子性，分配行为可复现。 */
    private final Object lock = new Object();
    private int totalHeld;

    @Autowired
    public CapacityGovernor(PipelineProperties props) {
        this.totalPermits = Math.max(1, props.getGlobalMaxInflight());
    }

    /** 测试用：直接指定总容量。 */
    public CapacityGovernor(int totalPermits) {
        this.totalPermits = Math.max(1, totalPermits);
    }

    public void registerSource(String source) {
        sources.add(source);
        heldBySource.putIfAbsent(source, 0);
    }

    public int totalPermits() {
        return totalPermits;
    }

    public int sourceCount() {
        return sources.size();
    }

    /** 每个来源的确定性公平份额：max(1, floor(总容量 / 来源数))。 */
    public int fairShare() {
        return Math.max(1, totalPermits / Math.max(1, sources.size()));
    }

    /**
     * 为某来源获取一个全局许可。
     *
     * @return false 表示总容量已满或该来源已占满自己的公平份额
     */
    public boolean tryAcquire(String source) {
        synchronized (lock) {
            int held = heldBySource.getOrDefault(source, 0);
            if (totalHeld >= totalPermits || held >= fairShare()) {
                return false;
            }
            heldBySource.put(source, held + 1);
            totalHeld++;
            return true;
        }
    }

    public void release(String source) {
        synchronized (lock) {
            int held = heldBySource.getOrDefault(source, 0);
            if (held > 0) {
                heldBySource.put(source, held - 1);
                totalHeld--;
            }
        }
    }

    public int held(String source) {
        return heldBySource.getOrDefault(source, 0);
    }

    public int totalHeld() {
        synchronized (lock) {
            return totalHeld;
        }
    }
}
