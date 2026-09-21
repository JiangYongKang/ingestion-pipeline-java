package com.github.highcumontoa.ingestionpipelinejava.source;

import java.util.List;

/**
 * 拉取式来源一次轮询的结果：严格按位点升序的记录集合。
 */
public class PollBatch {

    private final List<SourceRecord> records;

    public PollBatch(List<SourceRecord> records) {
        this.records = records;
    }

    public List<SourceRecord> getRecords() {
        return records;
    }
}
