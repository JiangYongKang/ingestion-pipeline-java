package com.github.highcumontoa.ingestionpipelinejava.store;

import com.github.highcumontoa.ingestionpipelinejava.model.SchemaDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个来源的结构演进历史：当前版本 + 历史版本（只读）+ 已删除列名。
 * removedColumns 用于识别“数据仍携带已删除列”的错列风险记录。
 */
public class SchemaState {

    private SchemaDefinition current;
    private List<SchemaDefinition> history = new ArrayList<>();
    private List<String> removedColumns = new ArrayList<>();

    public SchemaDefinition getCurrent() { return current; }
    public void setCurrent(SchemaDefinition current) { this.current = current; }

    public List<SchemaDefinition> getHistory() { return history; }
    public void setHistory(List<SchemaDefinition> history) { this.history = history; }

    public List<String> getRemovedColumns() { return removedColumns; }
    public void setRemovedColumns(List<String> removedColumns) { this.removedColumns = removedColumns; }
}
