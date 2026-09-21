package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 场景：新增列默认值稳定、非法演进拒绝、字段缺失/类型不匹配/删除列隔离、
 * 隔离记录修复后可重试、坏数据不中断整批且失败不污染已有状态。
 */
class SchemaEvolutionTest extends IngestionTestBase {

    private String sid;
    private String token;

    @BeforeEach
    void setup() {
        sid = "schema-" + System.nanoTime();
        token = "t-" + sid;
        registerSource(sid, "PUSH", token, null);
        registerBasicSchema(sid);
    }

    @Test
    @DisplayName("新增列带确定默认值：缺列记录按默认值落库，语义稳定")
    void addedColumnGetsStableDefault() {
        ObjectNode old = JsonFiles.mapper().createObjectNode();
        old.put("id", "u1");
        old.put("name", "alice");
        ensure2xx(push(sid, token, batch(List.of(item(1L, old)))), "old-shaped record");

        ObjectNode evolved = schema("id", List.of(
                Map.of("name", "id", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "name", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "age", "type", "LONG", "required", false, "defaultValue", "0"),
                Map.of("name", "active", "type", "BOOLEAN", "required", false, "defaultValue", "false"),
                Map.of("name", "score", "type", "DOUBLE", "required", false, "defaultValue", "1.5")
        ));
        JsonNode result = evolveSchema(sid, evolved);
        log.info("[JUDGE] schema version -> {}", result.get("version").asInt());
        assertEquals(2, result.get("version").asInt());

        ObjectNode partial = JsonFiles.mapper().createObjectNode();
        partial.put("id", "u2");
        partial.put("name", "bob");
        log.info("[INPUT] v2 record missing new columns: {}", partial);
        ensure2xx(push(sid, token, batch(List.of(item(2L, partial)))), "partial v2");

        JsonNode bob = findRow(getAdmin("/admin/sources/" + sid + "/data"), "u2");
        log.info("[JUDGE] normalized u2={}", bob.get("payload"));
        assertEquals(false, bob.get("payload").get("active").asBoolean());
        assertEquals(1.5, bob.get("payload").get("score").asDouble(), 0.0001);
        assertEquals(0, bob.get("payload").get("age").asLong());
    }

    @Test
    @DisplayName("新增列无默认值 / 改变既有列类型被拒绝：SCHEMA_EVOLUTION_INVALID")
    void invalidEvolutionRejected() {
        ObjectNode noDefault = schema("id", List.of(
                Map.of("name", "id", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "name", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "age", "type", "LONG", "required", false, "defaultValue", "0"),
                Map.of("name", "newcol", "type", "STRING", "required", false)
        ));
        ResponseEntity<JsonNode> r = postJson("/admin/sources/" + sid + "/schema/evolve", noDefault);
        log.info("[INPUT] no-default evolution -> status={}, code={}, msg={}",
                r.getStatusCode(), bodyCode(r), r.getBody().get("message").asText());
        assertEquals(400, r.getStatusCode().value());
        assertEquals("SCHEMA_EVOLUTION_INVALID", bodyCode(r));

        ObjectNode typeChange = schema("id", List.of(
                Map.of("name", "id", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "name", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "age", "type", "DOUBLE", "required", false, "defaultValue", "0")
        ));
        ResponseEntity<JsonNode> r2 = postJson("/admin/sources/" + sid + "/schema/evolve", typeChange);
        log.info("[INPUT] type-change evolution -> code={}", bodyCode(r2));
        assertEquals("SCHEMA_EVOLUTION_INVALID", bodyCode(r2));
    }

    @Test
    @DisplayName("缺必填/类型不匹配/携带已删除列逐条隔离，原因码可区分；位点越过坏行；修复后可重试")
    void badRowsQuarantinedAndCanBeRepaired() {
        // 演进：删除 age 列
        ObjectNode evolved = schema("id", List.of(
                Map.of("name", "id", "type", "STRING", "required", true, "defaultValue", ""),
                Map.of("name", "name", "type", "STRING", "required", true, "defaultValue", "")
        ));
        evolveSchema(sid, evolved);

        ObjectNode good = JsonFiles.mapper().createObjectNode();
        good.put("id", "u1");
        good.put("name", "alice");
        ObjectNode missingName = JsonFiles.mapper().createObjectNode();
        missingName.put("id", "u2");
        ObjectNode typeBad = JsonFiles.mapper().createObjectNode();
        typeBad.put("id", "u3");
        typeBad.put("name", 12345); // name 声明为 STRING，数字不做隐式转换 ⇒ 类型不匹配
        ObjectNode removed = JsonFiles.mapper().createObjectNode();
        removed.put("id", "u4");
        removed.put("name", "dan");
        removed.put("age", 40); // age 已被删除 ⇒ 携带已删除列（优先归类）

        ObjectNode mixed = batch(List.of(
                item(1L, good), item(2L, missingName), item(3L, typeBad), item(4L, removed)));
        log.info("[INPUT] mixed good/bad batch: {}", mixed);
        ResponseEntity<JsonNode> r = push(sid, token, mixed);
        ensure2xx(r, "mixed batch");
        JsonNode body = r.getBody();
        log.info("[JUDGE] inserted={}, quarantined={}, committed={}",
                body.get("inserted").asInt(), body.get("quarantined").asInt(),
                body.get("committedOffset").asLong());
        assertEquals(1, body.get("inserted").asInt());
        assertEquals(3, body.get("quarantined").asInt());
        assertEquals(4, body.get("committedOffset").asLong());

        JsonNode q = getAdmin("/admin/sources/" + sid + "/quarantine");
        for (JsonNode f : q) {
            log.info("[QUARANTINE] key={} code={} reason={} raw={}",
                    f.get("businessKey"), f.get("code").asText(),
                    f.get("reason").asText(), f.get("rawJson"));
        }
        assertEquals(1, countByCode(q, "SCHEMA_MISSING_FIELD"));
        assertEquals(1, countByCode(q, "SCHEMA_TYPE_MISMATCH"));
        assertEquals(1, countByCode(q, "SCHEMA_REMOVED_COLUMN"));

        // 坏数据不污染已有状态
        JsonNode data = getAdmin("/admin/sources/" + sid + "/data");
        assertEquals(1, data.size());

        // 修复 u2（补 name）后重试成功，移出奇队列
        ObjectNode fixed = JsonFiles.mapper().createObjectNode();
        fixed.put("id", "u2");
        fixed.put("name", "bob-repaired");
        log.info("[INPUT] retry u2 with repaired payload: {}", fixed);
        ResponseEntity<JsonNode> rr = postJson(
                "/admin/sources/" + sid + "/quarantine/u2/retry", fixed);
        ensure2xx(rr, "retry repaired u2");
        assertEquals(1, rr.getBody().get("inserted").asInt());
        JsonNode q2 = getAdmin("/admin/sources/" + sid + "/quarantine");
        assertEquals(2, q2.size(), "成功重试后应移出隔离区");
        assertEquals("bob-repaired", findRow(getAdmin("/admin/sources/" + sid + "/data"), "u2")
                .get("payload").get("name").asText());

        // u3 类型错误：重复重试累加 attempts，达到上限 exhausted，再次请求被 400 拒绝
        for (int i = 0; i < 3; i++) {
            postJson("/admin/sources/" + sid + "/quarantine/u3/retry", null);
        }
        JsonNode u3 = findQuarantine(getAdmin("/admin/sources/" + sid + "/quarantine"), "u3");
        log.info("[JUDGE] u3 attempts={} exhausted={}", u3.get("attempts").asInt(),
                u3.get("exhausted").asBoolean());
        assertEquals(3, u3.get("attempts").asInt());
        assertEquals(true, u3.get("exhausted").asBoolean());
        ResponseEntity<JsonNode> afterExhaust = postJson(
                "/admin/sources/" + sid + "/quarantine/u3/retry", null);
        log.info("[JUDGE] retry after exhausted -> status={}, code={}",
                afterExhaust.getStatusCode(), bodyCode(afterExhaust));
        assertEquals(400, afterExhaust.getStatusCode().value());
    }

    private ResponseEntity<JsonNode> postJson(String path, ObjectNode body) {
        return rest.exchange(base() + path, HttpMethod.POST,
                new HttpEntity<>(body, adminHeaders()), JsonNode.class);
    }

    private int countByCode(JsonNode arr, String code) {
        int n = 0;
        for (JsonNode f : arr) {
            if (code.equals(f.get("code").asText())) {
                n++;
            }
        }
        return n;
    }

    private JsonNode findRow(JsonNode arr, String key) {
        for (JsonNode row : arr) {
            if (key.equals(row.path("payload").path("id").asText())) {
                return row;
            }
        }
        throw new AssertionError("row not found: " + key);
    }

    private JsonNode findQuarantine(JsonNode arr, String key) {
        for (JsonNode f : arr) {
            if (key.equals(f.path("businessKey").asText())) {
                return f;
            }
        }
        throw new AssertionError("quarantine not found: " + key);
    }

    private String bodyCode(ResponseEntity<JsonNode> r) {
        return r.getBody() == null ? "null" : r.getBody().path("code").asText("null");
    }
}
