package com.github.highcumontoa.ingestionpipelinejava.core;

import com.github.highcumontoa.ingestionpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.source.PollBatch;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceConnector;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRecord;
import com.github.highcumontoa.ingestionpipelinejava.source.SourceRegistry;
import com.github.highcumontoa.ingestionpipelinejava.store.MetricsStore;
import com.github.highcumontoa.ingestionpipelinejava.store.OffsetStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 拉取执行器：执行一轮“从已确认位点之后轮询 → 连续前缀 → 核心处理”。
 * 独立于调度器存在，便于手动触发与测试；调度器只负责周期调用。
 */
@Component
public class PullRunner {

    private static final Logger log = LoggerFactory.getLogger(PullRunner.class);

    private final PipelineProperties props;
    private final SourceRegistry registry;
    private final OffsetStore offsets;
    private final IngestionService ingestion;
    private final MetricsStore metrics;

    public PullRunner(PipelineProperties props, SourceRegistry registry, OffsetStore offsets,
                      IngestionService ingestion, MetricsStore metrics) {
        this.props = props;
        this.registry = registry;
        this.offsets = offsets;
        this.ingestion = ingestion;
        this.metrics = metrics;
    }

    /** 对所有启用的拉取式来源执行一轮。 */
    public void runOnce() {
        for (SourceConfig cfg : registry.all()) {
            if (!cfg.isEnabled()) {
                continue;
            }
            SourceConnector connector = registry.require(cfg.getId());
            if (!connector.pullable()) {
                continue;
            }
            try {
                runSource(cfg.getId(), connector);
            } catch (Exception e) {
                log.warn("pull cycle failed for source '{}': {}", cfg.getId(), e.getMessage());
                try {
                    metrics.increment(cfg.getId(), m -> m.setRejected(m.getRejected() + 1));
                } catch (Exception ignore) {
                    // 计数不可用时不影响其它来源的拉取
                }
            }
        }
    }

    /** 对单个来源执行一轮，返回是否有记录被处理。 */
    public boolean runSource(String sourceId, SourceConnector connector) {
        long after = offsets.load(sourceId).getOffset();
        PollBatch batch = connector.poll(after);
        List<SourceRecord> records = batch.getRecords();
        if (records.isEmpty()) {
            return false;
        }
        // 只取从 after+1 起严格连续的前缀（最多一批）；缺口存在时等待补齐，绝不越过。
        List<SourceRecord> page = new ArrayList<>();
        long expect = after + 1;
        for (SourceRecord r : records) {
            if (r.getOffset() != expect || page.size() >= props.getMaxBatchSize()) {
                break;
            }
            page.add(r);
            expect++;
        }
        if (page.isEmpty()) {
            log.warn("source '{}' has a gap starting at offset {}; waiting for backfill",
                    sourceId, expect);
            return false;
        }
        log.info("pull source '{}' after={} available={} processing {}",
                sourceId, after, records.size(), page.size());
        ingestion.ingest(sourceId, page);
        return true;
    }
}
