package com.github.highcumontoa.ingestionpipelinejava.model;

import java.util.List;

/**
 * 推送批次（POST /ingest/{source} 请求体）。
 * schema 非空表示声明新结构版本；records 为该批数据。
 */
public record RecordBatch(String batchId,
                          RecordSchema schema,
                          List<IncomingRecord> records) {
}
