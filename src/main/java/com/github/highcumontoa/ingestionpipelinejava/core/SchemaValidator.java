package com.github.highcumontoa.ingestionpipelinejava.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.highcumontoa.ingestionpipelinejava.error.IngestionException;
import com.github.highcumontoa.ingestionpipelinejava.model.ColumnDef;
import com.github.highcumontoa.ingestionpipelinejava.model.ColumnType;
import com.github.highcumontoa.ingestionpipelinejava.model.ErrorCode;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaDefinition;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaState;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 按当前结构版本校验并规范化原始记录。
 *
 * <p>确定的判定规则：
 * <ul>
 *   <li>不是合法 JSON 对象 / 业务键缺失 ⇒ DATA_INVALID / SCHEMA_MISSING_FIELD，隔离；</li>
 *   <li>携带已删除列 ⇒ SCHEMA_REMOVED_COLUMN，隔离（防止错列/语义漂移）；</li>
 *   <li>必填列缺失 ⇒ SCHEMA_MISSING_FIELD，隔离；</li>
 *   <li>可选列缺失 ⇒ 以列定义中的 defaultValue 稳定补值；</li>
 *   <li>类型不匹配（字符串与数值/布尔之间不做隐式转换）⇒ SCHEMA_TYPE_MISMATCH，隔离；</li>
 *   <li>未在结构中声明的多余字段 ⇒ 确定性剔除（前向兼容），不参与哈希与存储。</li>
 * </ul>
 * 被隔离的记录不进入数据存储，也不影响位点之外的既有状态。
 */
public final class SchemaValidator {

    private SchemaValidator() {
    }

    /** 校验并输出仅含已声明列、按列顺序排列的规范化对象。 */
    public static ObjectNode normalize(SchemaState state, JsonNode raw) {
        SchemaDefinition schema = state.getCurrent();
        if (raw == null || !raw.isObject()) {
            throw new IngestionException(ErrorCode.DATA_INVALID, "record must be a JSON object");
        }
        JsonNode keyNode = raw.get(schema.getKeyField());
        if (keyNode == null || keyNode.isNull()) {
            throw new IngestionException(ErrorCode.SCHEMA_MISSING_FIELD,
                    "missing required key field '" + schema.getKeyField() + "'");
        }

        List<String> removedHits = new ArrayList<>();
        for (String removed : state.getRemovedColumns()) {
            if (raw.has(removed)) {
                removedHits.add(removed);
            }
        }
        if (!removedHits.isEmpty()) {
            throw new IngestionException(ErrorCode.SCHEMA_REMOVED_COLUMN,
                    "record carries removed column(s) " + removedHits
                            + "; remove them from the payload before resend");
        }

        ObjectNode out = com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles
                .mapper().createObjectNode();
        for (ColumnDef col : schema.getColumns()) {
            JsonNode v = raw.get(col.getName());
            if (v == null || v.isNull()) {
                if (col.isRequired()) {
                    throw new IngestionException(ErrorCode.SCHEMA_MISSING_FIELD,
                            "missing required field '" + col.getName() + "'");
                }
                putDefault(out, col);
                continue;
            }
            out.set(col.getName(), coerce(col, v));
        }
        // 未声明字段被确定性忽略（不落库、不参与哈希）。
        Set<String> declared = new java.util.HashSet<>();
        schema.getColumns().forEach(c -> declared.add(c.getName()));
        List<String> undeclared = new ArrayList<>();
        raw.fieldNames().forEachRemaining(name -> {
            if (!declared.contains(name)) {
                undeclared.add(name);
            }
        });
        if (!undeclared.isEmpty()) {
            // 仅作为原因的一部分可追溯；不拒绝、不落库。
            out.without(undeclared);
        }
        return out;
    }

    /** 解析业务键（键列强制字符串语义）。 */
    public static String businessKey(SchemaDefinition schema, JsonNode raw) {
        JsonNode v = raw.get(schema.getKeyField());
        if (v == null || v.isNull()) {
            throw new IngestionException(ErrorCode.SCHEMA_MISSING_FIELD,
                    "missing required key field '" + schema.getKeyField() + "'");
        }
        if (v.isTextual()) {
            return v.asText();
        }
        return v.toString();
    }

    private static JsonNode coerce(ColumnDef col, JsonNode v) {
        switch (col.getType()) {
            case STRING -> {
                if (!v.isTextual()) {
                    throw mismatch(col, v);
                }
                return v;
            }
            case LONG -> {
                if (v.isIntegralNumber() && v.canConvertToLong()) {
                    return v;
                }
                throw mismatch(col, v);
            }
            case DOUBLE -> {
                if (v.isNumber()) {
                    return v;
                }
                throw mismatch(col, v);
            }
            case BOOLEAN -> {
                if (v.isBoolean()) {
                    return v;
                }
                throw mismatch(col, v);
            }
            default -> throw new IngestionException(ErrorCode.SCHEMA_TYPE_MISMATCH,
                    "unknown column type for '" + col.getName() + "'");
        }
    }

    private static IngestionException mismatch(ColumnDef col, JsonNode v) {
        return new IngestionException(ErrorCode.SCHEMA_TYPE_MISMATCH,
                "field '" + col.getName() + "' expects " + col.getType()
                                + " but got " + nodeKind(v));
    }

    private static String nodeKind(JsonNode v) {
        return v.getNodeType().name().toLowerCase();
    }

    private static void putDefault(ObjectNode out, ColumnDef col) {
        String d = col.getDefaultValue();
        if (d == null) {
            out.putNull(col.getName());
            return;
        }
        switch (col.getType()) {
            case STRING -> out.put(col.getName(), d);
            case LONG -> {
                try {
                    out.put(col.getName(), Long.parseLong(d.trim()));
                } catch (NumberFormatException e) {
                    throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                            "invalid default for LONG column '" + col.getName() + "': " + d);
                }
            }
            case DOUBLE -> {
                try {
                    out.put(col.getName(), Double.parseDouble(d.trim()));
                } catch (NumberFormatException e) {
                    throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                            "invalid default for DOUBLE column '" + col.getName() + "': " + d);
                }
            }
            case BOOLEAN -> {
                String b = d.trim().toLowerCase();
                if (!b.equals("true") && !b.equals("false")) {
                    throw new IngestionException(ErrorCode.SCHEMA_EVOLUTION_INVALID,
                            "invalid default for BOOLEAN column '" + col.getName() + "': " + d);
                }
                out.put(col.getName(), Boolean.parseBoolean(b));
            }
        }
    }
}
