package com.github.highcumontoa.ingestionpipelinejava.config;

/** 单个来源的配置项（ingestion.sources[*]）。 */
public class SourceConfig {
    private String name;
    private String type;
    private String token;
    /** PULL_FILE: 记录文件所在目录 */
    private String dir;
    /** PULL_HTTP: 远端端点基础地址 */
    private String url;
    /** 每批最大记录数 */
    private Integer batchSize;
    /** 每秒令牌数（0 或空表示不限速） */
    private Integer rateLimitPerSec;
    /** 有界积压：最大在途请求数 */
    private Integer maxInflight;
    /** 拉取轮询间隔毫秒 */
    private Long pollIntervalMs;
    /** 单条记录最大尝试次数（含首次） */
    private Integer maxAttempts;
    private Boolean enabled = Boolean.TRUE;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }
    public String getDir() { return dir; }
    public void setDir(String dir) { this.dir = dir; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public Integer getBatchSize() { return batchSize; }
    public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }
    public Integer getRateLimitPerSec() { return rateLimitPerSec; }
    public void setRateLimitPerSec(Integer rateLimitPerSec) { this.rateLimitPerSec = rateLimitPerSec; }
    public Integer getMaxInflight() { return maxInflight; }
    public void setMaxInflight(Integer maxInflight) { this.maxInflight = maxInflight; }
    public Long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(Long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
    public Integer getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean enabled) { this.enabled = enabled; }
}
