package com.github.highcumontoa.ingestionpipelinejava.store;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 冲突证据：同业务键出现不同内容时，原样保留冲突版本，不覆盖既有数据。
 */
public class ConflictRecord {

    private String businessKey;
    private JsonNode incomingPayload;
    private String incomingHash;
    private String storedHash;
    private long incomingOffset;
    private long detectedAt;

    public ConflictRecord() {
    }

    public String getBusinessKey() { return businessKey; }
    public void setBusinessKey(String businessKey) { this.businessKey = businessKey; }

    public JsonNode getIncomingPayload() { return incomingPayload; }
    public void setIncomingPayload(JsonNode incomingPayload) { this.incomingPayload = incomingPayload; }

    public String getIncomingHash() { return incomingHash; }
    public void setIncomingHash(String incomingHash) { this.incomingHash = incomingHash; }

    public String getStoredHash() { return storedHash; }
    public void setStoredHash(String storedHash) { this.storedHash = storedHash; }

    public long getIncomingOffset() { return incomingOffset; }
    public void setIncomingOffset(long incomingOffset) { this.incomingOffset = incomingOffset; }

    public long getDetectedAt() { return detectedAt; }
    public void setDetectedAt(long detectedAt) { this.detectedAt = detectedAt; }
}
