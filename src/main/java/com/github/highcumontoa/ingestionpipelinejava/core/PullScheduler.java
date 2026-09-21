package com.github.highcumontoa.ingestionpipelinejava.core;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 拉取式来源的内置调度器：周期性调用 {@link PullRunner}。
 *
 * <p>位点在处理成功后由 IngestionService 落盘，调度器不记录位置，
 * 重启/中断后从上次已确认位置继续；同一来源的核心处理持锁串行，不会并发越位。
 * 调度开关 ingestion.pull-scheduler-enabled=false 时，仍可通过管理端点手动触发。
 */
@Component
@ConditionalOnProperty(name = "ingestion.pull-scheduler-enabled", havingValue = "true",
        matchIfMissing = true)
public class PullScheduler {

    private final PipelineProperties props;
    private final PullRunner runner;
    private ScheduledExecutorService executor;

    public PullScheduler(PipelineProperties props, PullRunner runner) {
        this.props = props;
        this.runner = runner;
    }

    @PostConstruct
    void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pull-scheduler");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(runner::runOnce,
                props.getPullIntervalMillis(), props.getPullIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
