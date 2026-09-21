package com.github.highcumontoa.ingestionpipelinejava.model;

/** 字段定义：名称、逻辑类型、是否必填。 */
public record SchemaField(String name, FieldType type, boolean required) {

    /** 支持的逻辑类型。新增字段的默认语义由 SchemaStore 统一确定。 */
    public enum FieldType {
        STRING, LONG, DOUBLE, BOOLEAN
    }
}
