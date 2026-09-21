package com.github.highcumontoa.ingestionpipelinejava.config;

/** 来源类型：推送式 / 文件拉取 / HTTP 拉取。 */
public enum SourceType {
    PUSH,
    PULL_FILE,
    PULL_HTTP;

    /** 解析配置中的类型字符串，非法值抛 IllegalArgumentException（由装配层转换为 UNKNOWN_SOURCE_TYPE）。 */
    public static SourceType parse(String raw) {
        return valueOf(raw.trim().toUpperCase());
    }
}
