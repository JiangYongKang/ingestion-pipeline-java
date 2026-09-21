package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 场景：拉取式来源（PULL_FILE 本地文件、PULL_HTTP 内置模拟端点）
 * 从上次确认位点继续消费；模拟“中断”时未确认的文件会在重新拉取时幂等处理，
 * 最终数据无重复。
 */
class PullSourceTest extends IngestionTestBase {

    @Test
    @DisplayName("PULL_FILE：写入编号文件 -> 手动拉取 -> 位点推进；新增文件后只消费新文件")
    void fileSourcePullsAndAdvancesOffset(@TempDir Path inbox) throws Exception {
        String sid = "file-" + System.nanoTime();
        String token = "t-" + sid;
        registerSource(sid, "PULL_FILE", token, java.util.Map.of("watchDir", inbox.toString()));
        registerBasicSchema(sid);

        writeFile(inbox, 1, record("f1", "alice", 10));
        writeFile(inbox, 2, record("f2", "bob", 11));
        // 非数字命名文件被忽略，不参与位点
        Files.writeString(inbox.resolve("README.txt"), "ignore me");

        postAdmin("/admin/pull/tick");
        JsonNode off1 = getAdmin("/admin/sources/" + sid + "/offset");
        JsonNode data1 = getAdmin("/admin/sources/" + sid + "/data");
        log.info("[JUDGE] after first tick offset={}, rows={}", off1.get("offset").asLong(), data1.size());
        assertEquals(2, off1.get("offset").asLong());
        assertEquals(2, data1.size());

        // 再写一批（位点 3、4），再次拉取只处理新增
        writeFile(inbox, 3, record("f3", "carol", 12));
        writeFile(inbox, 4, record("f4", "dan", 13));
        postAdmin("/admin/pull/tick");
        JsonNode off2 = getAdmin("/admin/sources/" + sid + "/offset");
        JsonNode data2 = getAdmin("/admin/sources/" + sid + "/data");
        log.info("[JUDGE] after second tick offset={}, rows={}", off2.get("offset").asLong(), data2.size());
        assertEquals(4, off2.get("offset").asLong());
        assertEquals(4, data2.size());

        // 再次空轮询：没有重复消费
        postAdmin("/admin/pull/tick");
        JsonNode metrics = getAdmin("/admin/sources/" + sid + "/metrics");
        log.info("[STATE] metrics received={}, inserted={}, duplicates={}",
                metrics.get("received").asLong(), metrics.get("inserted").asLong(),
                metrics.get("duplicates").asLong());
        assertEquals(4, getAdmin("/admin/sources/" + sid + "/data").size());
    }

    @Test
    @DisplayName("PULL_HTTP：向模拟源端投放 -> 拉取入库；重复轮询幂等；错误令牌被来源拒绝")
    void httpSourcePullsFromMockEndpoint() {
        String sid = "http-" + System.nanoTime();
        String token = "t-" + sid;
        registerSource(sid, "PULL_HTTP", token,
                java.util.Map.of("baseUrl", base() + "/mock/sources/" + sid));
        registerBasicSchema(sid);

        // 向模拟源端投放两条（源端自己分配单调位点）
        feedMock(sid, token, record("h1", "alice", 1));
        feedMock(sid, token, record("h2", "bob", 2));

        postAdmin("/admin/pull/tick");
        JsonNode off1 = getAdmin("/admin/sources/" + sid + "/offset");
        JsonNode data1 = getAdmin("/admin/sources/" + sid + "/data");
        log.info("[JUDGE] http after tick offset={}, rows={}", off1.get("offset").asLong(), data1.size());
        assertEquals(2, off1.get("offset").asLong());
        assertEquals(2, data1.size());

        // 再次 tick：连接器 after=2 拉不到新记录，不产生重复
        postAdmin("/admin/pull/tick");
        assertEquals(2, getAdmin("/admin/sources/" + sid + "/data").size());

        // 投放新记录后，只消费增量
        feedMock(sid, token, record("h3", "carol", 3));
        postAdmin("/admin/pull/tick");
        assertEquals(3, getAdmin("/admin/sources/" + sid + "/data").size());
        assertEquals(3, getAdmin("/admin/sources/" + sid + "/offset").get("offset").asLong());

        // 模拟来源端鉴权：用错误令牌投放被拒（403），未授权不产生数据
        org.springframework.http.HttpHeaders bad = new org.springframework.http.HttpHeaders();
        bad.setBearerAuth("wrong-token");
        org.springframework.http.ResponseEntity<JsonNode> r = rest.exchange(
                base() + "/mock/sources/" + sid + "/feed",
                org.springframework.http.HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(record("hx", "evil", 9), bad),
                JsonNode.class);
        log.info("[JUDGE] mock feed with wrong token -> status={}", r.getStatusCode());
        assertEquals(403, r.getStatusCode().value());
    }

    private void writeFile(Path dir, long offset, ObjectNode payload) throws Exception {
        String name = String.format("%06d.json", offset);
        Files.writeString(dir.resolve(name), payload.toString(), StandardCharsets.UTF_8);
        log.info("[INPUT] wrote file {}", name);
    }

    private void feedMock(String sid, String token, ObjectNode payload) {
        org.springframework.http.ResponseEntity<JsonNode> r = rest.exchange(
                base() + "/mock/sources/" + sid + "/feed",
                org.springframework.http.HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(payload, sourceHeaders(token)),
                JsonNode.class);
        ensure2xx(r, "feed mock");
        log.info("[INPUT] fed mock source {} -> offset {}", sid, r.getBody().get("offset"));
    }
}
