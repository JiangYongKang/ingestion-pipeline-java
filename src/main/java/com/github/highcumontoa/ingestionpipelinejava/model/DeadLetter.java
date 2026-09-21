package com.github.highcumontoa.ingestionpipelinejava.model;

import java.util.Map;

/** 失败记录（可变 POJO，便于重试时更新状态与次数）。 */
public class DeadLetter {
    private String source;
    private Long sequence;
    private String key;
    private Map<String, Object> data;
    private RejectReason reason;
    private String errorDetail;
    private int attempts;
    private Status status = Status.PENDING;
    private String firstFailedAt;
    private String lastUpdatedAt;

    public enum Status { PENDING, RETRIED, EXHAUSTED, RESOLVED }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public Long getSequence() { return sequence; }
    public void setSequence(Long sequence) { this.sequence = sequence; }
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }
    public RejectReason getReason() { return reason; }
    public void setReason(RejectReason reason) { this.reason = reason; }
    public String getErrorDetail() { return errorDetail; }
    public void setErrorDetail(String errorDetail) { this.errorDetail = errorDetail; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public String getFirstFailedAt() { return firstFailedAt; }
    public void setFirstFailedAt(String firstFailedAt) { this.firstFailedAt = firstFailedAt; }
    public String getLastUpdatedAt() { return lastUpdatedAt; }
    public void setLastUpdatedAt(String lastUpdatedAt) { this.lastUpdatedAt = lastUpdatedAt; }
}
