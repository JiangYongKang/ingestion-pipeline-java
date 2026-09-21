package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 已确认写入的记录：业务键、规范化内容、内容哈希与首次写入位点。
 */
public class StoredRecord {

    private String businessKey;
    private JsonNode payload;
    private String contentHash;
    private long firstOffset;
    private long writtenAt;

    public StoredRecord() {
    }

    public String getBusinessKey() { return businessKey; }
    public void setBusinessKey(String businessKey) { this.businessKey = businessKey; }

    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }

    public String getContentHash() { return contentHash; }
    public void setContentHash(String contentHash) { this.contentHash = contentHash; }

    public long getFirstOffset() { return firstOffset; }
    public void setFirstOffset(long firstOffset) { this.firstOffset = firstOffset; }

    public long getWrittenAt() { return writtenAt; }
    public void setWrittenAt(long writtenAt) { this.writtenAt = writtenAt; }
}
