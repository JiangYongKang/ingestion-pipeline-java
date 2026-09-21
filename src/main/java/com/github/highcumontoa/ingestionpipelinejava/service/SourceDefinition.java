package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceType;
import java.nio.file.Path;

/** 装配完成、校验通过后的来源运行时定义（不可变）。 */
public record SourceDefinition(String name,
                               SourceType type,
                               String token,
                               Path dir,
                               String url,
                               int batchSize,
                               int rateLimitPerSec,
                               int maxInflight,
                               long pollIntervalMs,
                               int maxAttempts) {

    public boolean push() {
        return type == SourceType.PUSH;
    }
}
