package com.github.highcumontoa.ingestionpipelinejava.source;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.model.SourceType;

/**
 * 依据配置装配连接器。类型未知 ⇒ SOURCE_TYPE_UNKNOWN；
 * 该类型必需字段缺失 ⇒ SOURCE_CONFIG_INVALID（两者错误码可区分）。
 */
public final class SourceConnectorFactory {

    private SourceConnectorFactory() {
    }

    public static SourceConnector create(SourceConfig cfg) {
        if (cfg == null) {
            throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID, "source config is null");
        }
        if (cfg.getId() == null || cfg.getId().isBlank()) {
            throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID, "source id is required");
        }
        SourceType type;
        try {
            type = SourceType.valueOf(cfg.getType());
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IngestionException(ErrorCode.SOURCE_TYPE_UNKNOWN,
                    "unknown source type '" + cfg.getType() + "' for source '" + cfg.getId()
                            + "'; supported: PUSH, PULL_FILE, PULL_HTTP");
        }
        if (cfg.getToken() == null || cfg.getToken().isBlank()) {
            throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID,
                    "token is required for source '" + cfg.getId() + "'");
        }
        return switch (type) {
            case PUSH -> new PushSourceConnector(cfg);
            case PULL_FILE -> {
                if (cfg.getWatchDir() == null || cfg.getWatchDir().isBlank()) {
                    throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID,
                            "watchDir is required for PULL_FILE source '" + cfg.getId() + "'");
                }
                yield new FileSourceConnector(cfg);
            }
            case PULL_HTTP -> {
                if (cfg.getBaseUrl() == null || cfg.getBaseUrl().isBlank()) {
                    throw new IngestionException(ErrorCode.SOURCE_CONFIG_INVALID,
                            "baseUrl is required for PULL_HTTP source '" + cfg.getId() + "'");
                }
                yield new HttpSourceConnector(cfg);
            }
        };
    }
}
