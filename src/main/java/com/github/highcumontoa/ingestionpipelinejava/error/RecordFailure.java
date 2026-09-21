package com.github.highcumontoa.ingestionpipelinejava.error;

import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;

/**
 * 单条记录处理失败时保留的原始信息与归类结果，进入隔离区/死信队列。
 */
public class RecordFailure {

    private String sourceId;
    private long offset;
    private String businessKey;
    private ErrorCode code;
    private String reason;
    private String rawJson;
    private int attempts;
    private long firstFailedAt;
    private long lastFailedAt;
    private boolean exhausted;

    public RecordFailure() {
    }

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public String getBusinessKey() { return businessKey; }
    public void setBusinessKey(String businessKey) { this.businessKey = businessKey; }

    public ErrorCode getCode() { return code; }
    public void setCode(ErrorCode code) { this.code = code; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    public String getRawJson() { return rawJson; }
    public void setRawJson(String rawJson) { this.rawJson = rawJson; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public long getFirstFailedAt() { return firstFailedAt; }
    public void setFirstFailedAt(long firstFailedAt) { this.firstFailedAt = firstFailedAt; }

    public long getLastFailedAt() { return lastFailedAt; }
    public void setLastFailedAt(long lastFailedAt) { this.lastFailedAt = lastFailedAt; }

    public boolean isExhausted() { return exhausted; }
    public void setExhausted(boolean exhausted) { this.exhausted = exhausted; }
}
