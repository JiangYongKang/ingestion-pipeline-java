package com.github.highcumontoa.ingestionpipelinejava.model;

/**
 * 每个来源可查询的处理计数：接收、首次写入、重复命中、冲突、
 * 隔离/拒绝、重试次数。计数持久化，重启后可延续观测。
 */
public class SourceMetrics {

    private String sourceId;
    private long received;
    private long inserted;
    private long duplicates;
    private long conflicts;
    private long quarantined;
    private long rejected;
    private long retried;

    public SourceMetrics() {
    }

    public SourceMetrics(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public long getReceived() { return received; }
    public void setReceived(long received) { this.received = received; }

    public long getInserted() { return inserted; }
    public void setInserted(long inserted) { this.inserted = inserted; }

    public long getDuplicates() { return duplicates; }
    public void setDuplicates(long duplicates) { this.duplicates = duplicates; }

    public long getConflicts() { return conflicts; }
    public void setConflicts(long conflicts) { this.conflicts = conflicts; }

    public long getQuarantined() { return quarantined; }
    public void setQuarantined(long quarantined) { this.quarantined = quarantined; }

    public long getRejected() { return rejected; }
    public void setRejected(long rejected) { this.rejected = rejected; }

    public long getRetried() { return retried; }
    public void setRetried(long retried) { this.retried = retried; }
}
