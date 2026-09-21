package com.github.highcumontoa.ingestionpipelinejava.model;

import java.util.List;

/** 批次处理结果：重放时 replayed=true 且直接返回首次结果。 */
public record BatchResult(String batchId, boolean replayed,
                         int accepted, int duplicate, int conflict, int quarantined,
                         List<RecordResult> records) {
}
