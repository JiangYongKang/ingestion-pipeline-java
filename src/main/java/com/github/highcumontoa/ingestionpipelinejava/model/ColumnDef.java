package com.github.highcumontoa.ingestionpipelinejava.model;

/**
 * 列定义。新增列通过 defaultValue 提供确定且稳定的默认语义：
 * 旧记录读取时始终以该默认值补列，不依赖写入时的环境状态。
 */
public class ColumnDef {

    private String name;
    private ColumnType type;
    private boolean required;
    /** 以字符串承载的默认值，按列类型解析；null 表示默认即 null。 */
    private String defaultValue;

    public ColumnDef() {
    }

    public ColumnDef(String name, ColumnType type, boolean required, String defaultValue) {
        this.name = name;
        this.type = type;
        this.required = required;
        this.defaultValue = defaultValue;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public ColumnType getType() { return type; }
    public void setType(ColumnType type) { this.type = type; }

    public boolean isRequired() { return required; }
    public void setRequired(boolean required) { this.required = required; }

    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
}
