package com.github.highcumontoa.ingestionpipelinejava.model;

/**
 * 可装配的来源类型：PUSH 推送式；PULL_FILE / PULL_HTTP 拉取式。
 */
public enum SourceType {
    PUSH,
    PULL_FILE,
    PULL_HTTP
}
