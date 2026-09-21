package com.github.highcumontoa.ingestionpipelinejava.source;

/**
 * 来源侧的一条原始记录：单调位点 + 原始 JSON 文本。
 */
public class SourceRecord {

    private final long offset;
    private final String rawJson;

    public SourceRecord(long offset, String rawJson) {
        this.offset = offset;
        this.rawJson = rawJson;
    }

    public long getOffset() { return offset;
    }

    public String getRawJson() { return rawJson; }
    }
