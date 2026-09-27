package com.github.highcumontoa.ingestionpipelinejava.config;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** ingestion.* 配置：状态目录与来源列表。 */
@Component
@ConfigurationProperties(prefix = "ingestion")
public class PipelineProperties {
    /** 位点 / 数据 / DLQ / schema / 指标 的持久化根目录 */
    private Path stateDir = Path.of("data");
    private List<SourceConfig> sources = new ArrayList<>();
    private int defaultBatchSize = 100;
    private int defaultMaxInflight = 1000;
    private int defaultMaxAttempts = 3;
    private long defaultPollIntervalMs = 500;
    /** 进程级总容量：所有来源（推送+拉取）合计的在途处理许可上限 */
    private int globalMaxInflight = 10000;

    public Path getStateDir() { return stateDir; }
    public void setStateDir(Path stateDir) { this.stateDir = stateDir; }
    public List<SourceConfig> getSources() { return sources; }
    public void setSources(List<SourceConfig> sources) { this.sources = sources; }
    public int getDefaultBatchSize() { return defaultBatchSize; }
    public void setDefaultBatchSize(int defaultBatchSize) { this.defaultBatchSize = defaultBatchSize; }
    public int getDefaultMaxInflight() { return defaultMaxInflight; }
    public void setDefaultMaxInflight(int defaultMaxInflight) { this.defaultMaxInflight = defaultMaxInflight; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }
    public long getDefaultPollIntervalMs() { return defaultPollIntervalMs; }
    public void setDefaultPollIntervalMs(long defaultPollIntervalMs) { this.defaultPollIntervalMs = defaultPollIntervalMs; }
    public int getGlobalMaxInflight() { return globalMaxInflight; }
    public void setGlobalMaxInflight(int globalMaxInflight) { this.globalMaxInflight = globalMaxInflight; }
}
