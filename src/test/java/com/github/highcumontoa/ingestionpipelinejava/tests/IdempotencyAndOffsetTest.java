package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.highcumontoa.ingestionpipelinejava.store.JsonFiles;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：重复投递/整批重放幂等、位点回退与非连续推进拒绝、同键不同内容冲突标记。
 */
class IdempotencyAndOffsetTest extends IngestionTestBase {

    private String sid;
    private String token;

    @BeforeEach
    void setup() {
        sid = "push-idem-" + System.nanoTime();
        token = "token-" + sid;
        registerSource(sid, "PUSH", token, null);
        registerBasicSchema(sid);
    }

    @Test
    @DisplayName("重复投递与整批重放不产生重复数据；位点只推进一次")
    void duplicateDeliveryIsIdempotent() {
        ObjectNode first = batch(List.of(
                item(1L, record("u1", "alice", 30)),
                item(2L, record("u2", "bob", 31))));
        log.info("[INPUT] first batch: {}", first);
        ResponseEntity<JsonNode> r1 = push(sid, token, first);
        ensure2xx(r1, "first batch");
        JsonNode b1 = r1.getBody();
        log.info("[JUDGE] first => status={}, inserted={}, committed={}",
                r1.getStatusCode(), b1.get("inserted").asInt(), b1.get("committedOffset").asLong());
        assertEquals(2, b1.get("inserted").asInt());
        assertEquals(2, b1.get("committedOffset").asLong());

        // 整批重放：相同内容再来一次，必须全部 DUPLICATE，位点不变，无新增数据
        ResponseEntity<JsonNode> r2 = push(sid, token, first);
        ensure2xx(r2, "replay batch");
        JsonNode b2 = r2.getBody();
        log.info("[JUDGE] replay => duplicates={}, committed={}",
                b2.get("duplicates").asInt(), b2.get("committedOffset").asLong());
        assertEquals(2, b2.get("duplicates").asInt());
        assertEquals(0, b2.get("inserted").asInt());
        assertEquals(2, b2.get("committedOffset").asLong());

        // 字段顺序不同、内容相同也必须判重（规范化哈希）
        ObjectNode reordered = JsonFiles.mapper().createObjectNode();
        reordered.put("age", 30);
        reordered.put("name", "alice");
        reordered.put("id", "u1");
        ObjectNode replayOne = batch(List.of(item(1L, reordered)));
        log.info("[INPUT] reordered replay: {}", replayOne);
        ResponseEntity<JsonNode> r3 = push(sid, token, replayOne);
        ensure2xx(r3, "reordered replay");
        assertEquals(1, r3.getBody().get("duplicates").asInt());

        JsonNode data = getAdmin("/admin/sources/" + sid + "/data");
        log.info("[STATE] final data rows={}", data.size());
        assertEquals(2, data.size(), "重放不得产生重复行");
    }

    @Test
    @DisplayName("位点回退与部分重叠被拒绝，且不写入、不推进")
    void offsetFallbackAndOverlapRejected() {
        ObjectNode batch12 = batch(List.of(
                item(1L, record("u1", "alice", 30)),
                item(2L, record("u2", "bob", 31))));
        push(sid, token, batch12);

        // 回退：从旧位点开始且部分重叠
        ObjectNode overlap = batch(List.of(
                item(2L, record("u2", "bob", 31)),
                item(3L, record("u3", "carol", 40))));
        log.info("[INPUT] overlapping batch: {}", overlap);
        ResponseEntity<JsonNode> r = push(sid, token, overlap);
        log.info("[JUDGE] overlap => status={}, body={}", r.getStatusCode(), r.getBody());
        assertEquals(409, r.getStatusCode().value());
        assertEquals("OFFSET_FALLBACK", r.getBody().get("code").asText());

        // 跳号：committed=2，却从 4 开始
        ObjectNode gap = batch(List.of(item(4L, record("u4", "dan", 50))));
        log.info("[INPUT] gap batch: {}", gap);
        ResponseEntity<JsonNode> rg = push(sid, token, gap);
        log.info("[JUDGE] gap => status={}, body={}", rg.getStatusCode(), rg.getBody());
        assertEquals(409, rg.getStatusCode().value());
        assertEquals("OFFSET_NON_MONOTONIC", rg.getBody().get("code").asText());

        // 批内不连续
        ObjectNode intra = batch(List.of(
                item(3L, record("u3", "carol", 40)),
                item(5L, record("u5", "erin", 22))));
        ResponseEntity<JsonNode> ri = push(sid, token, intra);
        log.info("[JUDGE] intra-gap => status={}, code={}", ri.getStatusCode(),
                ri.getBody().get("code").asText());
        assertEquals("OFFSET_NON_MONOTONIC", ri.getBody().get("code").asText());

        JsonNode offset = getAdmin("/admin/sources/" + sid + "/offset");
        JsonNode data = getAdmin("/admin/sources/" + sid + "/data");
        log.info("[STATE] rejected batches leave committed={}, rows={}",
                offset.get("offset").asLong(), data.size());
        assertEquals(2, offset.get("offset").asLong(), "拒绝后位点必须保持不变");
        assertEquals(2, data.size(), "被拒绝批次不得写入任何数据");
    }

    @Test
    @DisplayName("同一业务键出现不同内容标记为 CONFLICT：不覆盖不丢弃，保留证据")
    void sameKeyDifferentContentConflicts() {
        ObjectNode v1 = batch(List.of(item(1L, record("u1", "alice", 30))));
        push(sid, token, v1);

        ObjectNode v2 = batch(List.of(
                item(2L, record("u2", "bob", 31)),
                item(3L, record("u1", "ALICE-CHANGED", 31))));
        log.info("[INPUT] conflicting batch: {}", v2);
        ResponseEntity<JsonNode> r = push(sid, token, v2);
        ensure2xx(r, "conflicting batch accepted with per-record isolation");
        JsonNode body = r.getBody();
        log.info("[JUDGE] inserted={}, conflicts={}, quarantined={}",
                body.get("inserted").asInt(), body.get("conflicts").asInt(),
                body.get("quarantined").asInt());
        assertEquals(1, body.get("inserted").asInt());
        assertEquals(1, body.get("conflicts").asInt());

        // 原有 u1 内容保持不变（不静默覆盖）
        JsonNode data = getAdmin("/admin/sources/" + sid + "/data");
        String storedName = null;
        for (JsonNode row : data) {
            if ("u1".equals(row.path("payload").path("id").asText())) {
                storedName = row.path("payload").path("name").asText();
            }
        }
        log.info("[STATE] stored u1.name='{}' (must remain 'alice')", storedName);
        assertEquals("alice", storedName);

        JsonNode conflicts = getAdmin("/admin/sources/" + sid + "/conflicts");
        assertEquals(1, conflicts.size(), "冲突证据保留一条");
        assertTrue(conflicts.get(0).path("incomingPayload").path("name").asText().contains("CHANGED"));

        // 重放整批不会重复记冲突
        ResponseEntity<JsonNode> replay = push(sid, token, v2);
        ensure2xx(replay, "replay conflict batch");
        JsonNode conflicts2 = getAdmin("/admin/sources/" + sid + "/conflicts");
        log.info("[JUDGE] conflicts after replay={} (must stay 1)", conflicts2.size());
        assertEquals(1, conflicts2.size());
    }
}
