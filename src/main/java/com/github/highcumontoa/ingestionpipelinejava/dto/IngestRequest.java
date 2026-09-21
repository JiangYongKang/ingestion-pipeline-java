package com.github.highcumontoa.ingestionpipelinejava.dto;

import java.util.List;

/**
 * 接入请求：一批记录。批量规模受 ingestion.max-batch-size 限制。
 */
public class IngestRequest {

    private List<IngestItem> records;

    public List<IngestItem> getRecords() { return records; }
    public void setRecords(List<IngestItem> records) { this.records = records; }
}
