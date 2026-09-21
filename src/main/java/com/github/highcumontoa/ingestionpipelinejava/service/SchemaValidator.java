package com.github.highcumontoa.ingestionpipelinejava.service;

import com.github.highcumontoa.ingestionpipelinejava.model.DeadLetter;
import com.github.highcumontoa.ingestionpipelinejava.model.IncomingRecord;
import com.github.highcumontoa.ingestionpipelinejava.model.RecordSchema;
import com.github.highcumontoa.ingestionpipelinejava.model.RejectReason;
import com.github.highcumontoa.ingestionpipelinejava.model.SchemaField;
import com.github.highcumontoa.ingestionpipelinejava.store.DeadLetterStore;
import com.github.highcumontoa.ingestionpipelinejava.store.SchemaStore;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * 结构校验与演进入口：
 * <ol>
 *   <li>声明结构委托 SchemaStore（版本只能 +1；新增字段默认 null；删除列留痕）；</li>
 *   <li>逐列校验记录：COLUMN_DROPPED（出现已删除列）、UNKNOWN_FIELD（出现未声明列）、
 *       MISSING_FIELD（必填缺失/显式 null）、TYPE_MISMATCH（逻辑类型不匹配），互不混淆。</li>
 * </ol>
 * 校验失败的记录由调用方隔离，不造成错列或静默丢失。
 */
@Component
public class SchemaValidator {

    private final SchemaStore schemas;

    public SchemaValidator(SchemaStore schemas) {
        this.schemas = schemas;
    }

    /** 声明/演进结构，返回新版本号。 */
    public int declare(String source, RecordSchema declared) {
        return schemas.register(source, declared).version();
    }

    /**
     * 校验一条记录。
     *
     * @param detailOut 若非 null，失败时写入 {field, expected, actual} 等判定依据
     * @return 失败归类；null 表示通过
     */
    public RejectReason validate(String source, IncomingRecord record, Map<String, Object> detailOut) {
        if (!schemas.exists(source)) {
            detail(detailOut, "reason", "no schema declared for source yet");
            return RejectReason.SCHEMA_VERSION_INVALID;
        }
        RecordSchema schema = schemas.latest(source);
        Set<String> dropped = schemas.droppedFields(source);
        Map<String, Object> data = record.data() == null ? Map.of() : record.data();

        // 1) 已删除列再出现：直接隔离，绝不错列
        for (String col : dropped) {
            if (data.containsKey(col)) {
                detail(detailOut, "field", col);
                detail(detailOut, "schemaVersion", schema.version());
                return RejectReason.COLUMN_DROPPED;
            }
        }

        Map<String, SchemaField> byName = new LinkedHashMap<>();
        for (SchemaField f : schema.fields()) {
            byName.put(f.name(), f);
        }

        // 2) 未声明列：隔离，避免静默丢弃或错列
        for (String key : data.keySet()) {
            if (!byName.containsKey(key)) {
                detail(detailOut, "field", key);
                detail(detailOut, "schemaVersion", schema.version());
                return RejectReason.UNKNOWN_FIELD;
            }
        }

        // 3) 必填缺失或显式 null
        for (SchemaField f : schema.fields()) {
            if (f.required() && (data.get(f.name()) == null)) {
                detail(detailOut, "field", f.name());
                detail(detailOut, "schemaVersion", schema.version());
                return RejectReason.MISSING_FIELD;
            }
        }

        // 4) 类型检查（可选字段缺省 null 合法——新增字段的稳定默认语义）
        for (SchemaField f : schema.fields()) {
            Object v = data.get(f.name());
            if (v == null) {
                continue;
            }
            if (!matches(v, f.type())) {
                detail(detailOut, "field", f.name());
                detail(detailOut, "expected", f.type().name());
                detail(detailOut, "actual", v.getClass().getSimpleName());
                detail(detailOut, "schemaVersion", schema.version());
                return RejectReason.TYPE_MISMATCH;
            }
        }
        return null;
    }

    private static boolean matches(Object v, SchemaField.FieldType type) {
        return switch (type) {
            case STRING -> v instanceof String;
            case LONG -> (v instanceof Integer || v instanceof Long)
                    || (v instanceof Number n && n.doubleValue() == n.longValue()
                        && !(v instanceof Float) && !(v instanceof Double));
            case DOUBLE -> v instanceof Number;
            case BOOLEAN -> v instanceof Boolean;
        };
    }

    private static void detail(Map<String, Object> out, String k, Object v) {
        if (out != null) {
            out.put(k, v);
        }
    }

    /** 构造隔离记录（保留原始数据、原因与详细判定依据）。 */
    public DeadLetter toDeadLetter(String source, Long seq, IncomingRecord r,
                                   RejectReason reason, String detail, int attempts) {
        DeadLetter d = new DeadLetter();
        d.setSource(source);
        d.setSequence(seq);
        if (r != null) {
            d.setKey(r.key());
            d.setData(r.data() == null ? Map.of() : new LinkedHashMap<>(r.data()));
        }
        d.setReason(reason);
        d.setErrorDetail(detail);
        d.setAttempts(attempts);
        d.setStatus(DeadLetter.Status.PENDING);
        String now = Instant.now().toString();
        d.setFirstFailedAt(now);
        d.setLastUpdatedAt(now);
        return d;
    }
}
