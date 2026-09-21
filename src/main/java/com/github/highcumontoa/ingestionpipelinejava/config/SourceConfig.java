package com.github.highcumontoa.ingestionpipelinejava.config;

/**
 * 单个来源的配置。type 决定装配的来源类型；不同类型所需字段不同，
 * 不完整的配置在装配阶段会被明确拒绝（错误码 SOURCE_CONFIG_INVALID）。
 */
public class SourceConfig {

    private String id;
    private String type;
    /** 推送接入与来源侧鉴权令牌。 */
    private String token;
    /** PULL_FILE：被监听的本地目录。 */
    private String watchDir;
    /** PULL_HTTP：来源端点基址。 */
    private String baseUrl;
    /** 拉取来源的业务键字段（结构校验需要）。 */
    private String keyField;
    /** 注册后是否立即参与调度/接收。 */
    private boolean enabled = true;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }

    public String getWatchDir() { return watchDir; }
    public void setWatchDir(String watchDir) { this.watchDir = watchDir; }

    public String getBaseUrl() { return baseUrl; }
    public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

    public String getKeyField() { return keyField; }
    public void setKeyField(String keyField) { this.keyField = keyField; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
