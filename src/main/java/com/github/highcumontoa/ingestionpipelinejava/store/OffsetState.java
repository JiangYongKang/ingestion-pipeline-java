package com.github.highcumontoa.ingestionpipelinejava.store;

/**
 * 单个来源的持久化位点状态。
 * offset 为最后已确认位置；version 在每次成功推进后递增，用于并发 CAS。
 */
public class OffsetState {

    private long offset;
    private long version;

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
