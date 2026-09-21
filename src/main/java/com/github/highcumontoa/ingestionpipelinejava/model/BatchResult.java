package com.github.highcumontoa.ingestionpipelinejava.model;

import com.github.highcumontoa.ingestionpipelinejava.error.RecordFailure;

import java.util.ArrayList;
import java.util.List;

/**
 * 一批数据处理后的可解释结果：推进到的位点、各类计数与逐条失败明细。
 */
public class BatchResult {

    private String sourceId;
    private long fromOffset;
    private long committedOffset;
    private int received;
    private int inserted;
    private int duplicates;
    private int conflicts;
    private int quarantined;
    private final List<RecordFailure> failures = new ArrayList<>();

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public long getFromOffset() { return fromOffset; }
    public void setFromOffset(long fromOffset) { this.fromOffset = fromOffset; }

    public long getCommittedOffset() { return committedOffset; }
    public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }

    public int getReceived() { return received; }
    public void setReceived(int received) { this.received = received; }

    public int getInserted() { return inserted; }
    public void setInserted(int inserted) { this.inserted = inserted; }

    public int getDuplicates() { return duplicates; }
    public void setDuplicates(int duplicates) { this.duplicates = duplicates; }

    public int getConflicts() { return conflicts; }
    public void setConflicts(int conflicts) { this.conflicts = conflicts; }

    public int getQuarantined() { return quarantined; }
    public void setQuarantined(int quarantined) { this.quarantined = quarantined; }

    public List<RecordFailure> getFailures() { return failures; }
}
