package com.github.highcumontoa.ingestionpipelinejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 某来源在某个版本下的结构。版本单调递增；keyField 为业务键列。
 */
public class SchemaDefinition {

    private int version;
    private String keyField;
    private List<ColumnDef> columns = new ArrayList<>();

    public SchemaDefinition() {
    }

    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }

    public String getKeyField() { return keyField; }
    public void setKeyField(String keyField) { this.keyField = keyField; }

    public List<ColumnDef> getColumns() { return columns; }
    public void setColumns(List<ColumnDef> columns) { this.columns = columns; }
}
