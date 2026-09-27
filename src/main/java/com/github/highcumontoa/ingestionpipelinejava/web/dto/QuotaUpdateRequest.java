package com.github.highcumontoa.ingestionpipelinejava.web.dto;

/**
 * 配额热调整请求体；null 字段表示保持不变。
 * 非法值（batchSize<=0、rateLimitPerSec<0、maxInflight<=0）整体拒绝，当前生效配置不变。
 */
public record QuotaUpdateRequest(Integer batchSize, Integer rateLimitPerSec, Integer maxInflight) {
}
