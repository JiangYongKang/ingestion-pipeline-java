package com.github.highcumontoa.ingestionpipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * 全局可配置项：数据目录、批量规模、积压策略、重试上限与拉取调度开关。
 */
@ConfigurationProperties(prefix = "ingestion")
public class PipelineProperties {

    /** 持久化根目录（位点、存储、隔离记录均落盘于此）。 */
    private String dataDir = "data/ingestion";

    /** 单次批量最大记录数（超出拒绝，保证处理有界、可及时返回）。 */
    private int maxBatchSize = 100;

    /** 每个来源待确认写入的在途记录上限（积压阈值，内存有界）。 */
    private int backlogLimit = 200;

    /** 积压处置策略：REJECT 直接拒绝 / RATE_LIMIT 限速等待。 */
    private String backlogPolicy = "REJECT";

    /** RATE_LIMIT 策略下的准入等待上限（毫秒）。 */
    private long backlogAwaitMillis = 2000;

    /** 单条失败记录的最大重试次数，超过后不再自动重投。 */
    private int maxRetryAttempts = 3;

    /** 是否启动拉取式来源的内置调度器。 */
    private boolean pullSchedulerEnabled = true;

    /** 拉取调度的固定间隔（毫秒）。 */
    private long pullIntervalMillis = 5000;

    /** 管理端点的管理员令牌（缺省时拒绝全部管理操作）。 */
    private String adminToken = "";

    /** 启动时装配的来源（也可通过管理端点动态注册）。 */
    private List<SourceConfig> sources = new ArrayList<>();

    public List<SourceConfig> getSources() { return sources; }
    public void setSources(List<SourceConfig> sources) { this.sources = sources; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public int getMaxBatchSize() { return maxBatchSize; }
    public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }

    public int getBacklogLimit() { return backlogLimit; }
    public void setBacklogLimit(int backlogLimit) { this.backlogLimit = backlogLimit; }

    public String getBacklogPolicy() { return backlogPolicy; }
    public void setBacklogPolicy(String backlogPolicy) { this.backlogPolicy = backlogPolicy; }

    public long getBacklogAwaitMillis() { return backlogAwaitMillis; }
    public void setBacklogAwaitMillis(long backlogAwaitMillis) { this.backlogAwaitMillis = backlogAwaitMillis; }

    public int getMaxRetryAttempts() { return maxRetryAttempts; }
    public void setMaxRetryAttempts(int maxRetryAttempts) { this.maxRetryAttempts = maxRetryAttempts; }

    public boolean isPullSchedulerEnabled() { return pullSchedulerEnabled; }
    public void setPullSchedulerEnabled(boolean pullSchedulerEnabled) { this.pullSchedulerEnabled = pullSchedulerEnabled; }

    public long getPullIntervalMillis() { return pullIntervalMillis; }
    public void setPullIntervalMillis(long pullIntervalMillis) { this.pullIntervalMillis = pullIntervalMillis; }

    public String getAdminToken() { return adminToken; }
    public void setAdminToken(String adminToken) { this.adminToken = adminToken; }
}
