package com.github.highcumontoa.ingestionpipelinejava.model;

import java.util.List;

/**
 * 一个来源在某个版本上的结构定义。
 * 演进规则：版本只能 +1；允许新增非必填字段（默认 null）；
 * 被删除的字段以 dropped=true 标记，旧数据中再出现该列即隔离。
 */
public record RecordSchema(int version, List<SchemaField> fields) {
}
