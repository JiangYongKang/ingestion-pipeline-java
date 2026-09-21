package com.github.highcumontoa.ingestionpipelinejava.source;

import com.github.highcumontoa.ingestionpipelinejava.config.SourceConfig;

/**
 * 推送式来源：数据由对端 POST 到接入端点，本连接器不参与轮询。
 */
public class PushSourceConnector implements SourceConnector {

    private final SourceConfig config;

    public PushSourceConnector(SourceConfig config) {
        this.config = config;
    }

    @Override
    public SourceConfig config() {
        return config;
    }
}
