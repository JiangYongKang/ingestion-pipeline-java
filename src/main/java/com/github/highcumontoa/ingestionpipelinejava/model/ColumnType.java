package com.github.highcumontoa.ingestionpipelinejava.model;

/**
 * 受支持的列类型。类型不匹配且无法按稳定规则转换时，记录被隔离。
 */
public enum ColumnType {
    STRING,
    LONG,
    DOUBLE,
    BOOLEAN
}
