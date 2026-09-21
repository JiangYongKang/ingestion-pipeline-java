package com.github.highcumontoa.ingestionpipelinejava.model;

/** 单条记录判定结果。 */
public record RecordResult(String key, Status status, RejectReason reason, String detail) {

    public enum Status { ACCEPTED, DUPLICATE, CONFLICT, QUARANTINED }

    public static RecordResult accepted(String key) {
        return new RecordResult(key, Status.ACCEPTED, null, null);
    }
    public static RecordResult duplicate(String key) {
        return new RecordResult(key, Status.DUPLICATE, null, "identical content already stored");
    }
    public static RecordResult conflict(String key, String detail) {
        return new RecordResult(key, Status.CONFLICT, RejectReason.RECORD_CONFLICT, detail);
    }
    public static RecordResult quarantined(String key, RejectReason reason, String detail) {
        return new RecordResult(key, Status.QUARANTINED, reason, detail);
    }
}
