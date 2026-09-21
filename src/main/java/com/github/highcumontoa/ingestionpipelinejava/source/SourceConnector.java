package com.github.highcumontoa.ingestionpipelinejava.source;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;

/**
 * 可插拔来源连接器。装配由 SourceConnectorFactory 依据配置完成；
 * 仅拉取式连接器需要实现 poll，推送式接入由 HTTP 端点直接承接。
 */
public interface SourceConnector {

    /** 该连接器服务的来源配置。 */
    SourceConfig config();

    /** 是否为拉取式连接器。 */
    default boolean pullable() {
        return false;
    }

    /**
     * 从严格大于 afterOffset 的位点拉取一批记录，按位点升序返回。
     */
    default PollBatch poll(long afterOffset) {
        throw new UnsupportedOperationException("connector is not pullable");
    }
}
