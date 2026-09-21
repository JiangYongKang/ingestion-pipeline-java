package com.github.highcumontoa.ingestionpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：多线程并发推进同一来源不得跳变/越位/错位；
 * 积压超限时按 REJECT 策略有界拒绝；批量超上限被拒绝。
 */
class ConcurrencyAndBacklogTest extends IngestionTestBase {

    private String sid;
    private String token;

    @BeforeEach
    void setup() {
        sid = "conc-" + System.nanoTime();
        token = "t-" + sid;
        registerSource(sid, "PUSH", token, null);
        registerBasicSchema(sid);
    }

    @Test
    @DisplayName("并发分区提交：只有恰好连续的分区成功，其余以位点原因拒绝，无跳变无重复")
    void concurrentPartitionedCommitsAreConsistent() throws Exception {
        int threads = 12;
        int perThread = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        ConcurrentLinkedQueue<String> acceptedCodes = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            futures.add(pool.submit(() -> {
                await(start);
                List<Map.Entry<Long, ObjectNode>> items = new ArrayList<>();
                for (long o = base + 1; o <= base + perThread; o++) {
                    items.add(item(o, record("k-" + o, "name-" + o, o)));
                }
                ResponseEntity<JsonNode> r = push(sid, token, batch(items));
                if (r.getStatusCode().is2xxSuccessful()) {
                    acceptedCodes.add("OK");
                    log.info("[CONCUR] partition [{},{}] committed={}", base + 1, base + perThread,
                            r.getBody().get("committedOffset").asLong());
                } else {
                    acceptedCodes.add(r.getBody().get("code").asText());
                    log.info("[CONCUR] partition [{},{}] rejected code={} msg={}", base + 1,
                            base + perThread, r.getBody().get("code").asText(),
                            r.getBody().get("message").asText());
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        long okCount = acceptedCodes.stream().filter("OK"::equals).count();
        JsonNode offset = getAdmin("/admin/sources/" + sid + "/offset");
        JsonNode data = getAdmin("/admin/sources/" + sid + "/data");
        log.info("[JUDGE] accepted partitions={}, final committed={}, rows={}",
                okCount, offset.get("offset").asLong(), data.size());

        // 关键不变量：位点必须等于成功分区连续覆盖的长度；数据行数 == 位点（无重复、无丢失）
        assertEquals(okCount * perThread, offset.get("offset").asLong(),
                "位点必须精确等于成功分区覆盖的连续长度");
        assertEquals(offset.get("offset").asLong(), data.size(),
                "数据行数必须等于已确认位点，不允许重复或错位");
        Set<String> keys = new HashSet<>();
        for (JsonNode row : data) {
            keys.add(row.path("payload").path("id").asText());
        }
        assertEquals((int) offset.get("offset").asLong(), keys.size(), "业务键无重复");
        assertTrue(okCount >= 1, "至少一个分区必须成功");
        assertTrue(okCount < threads, "乱序并发下不可能所有分区都成功");
    }

    @Test
    @DisplayName("积压上限：超过 backlog-limit 的在途批量得到 429 BACKLOG_LIMIT_REACHED")
    void backlogRejectIsBoundedAndObservable() throws Exception {
        // backlog-limit=25；用一个受控的“慢处理”无法轻易注入，改为直接验证批量超过上限：
        // 单批 26 > 25 ⇒ 积压拒绝（无需并发即可复现有界保证）。
        List<Map.Entry<Long, ObjectNode>> items = new ArrayList<>();
        for (long o = 1; o <= 26; o++) {
            items.add(item(o, record("big-" + o, "n", o)));
        }
        ResponseEntity<JsonNode> r = push(sid, token, batch(items));
        log.info("[INPUT] batch size 26 over limit 25 -> status={}, code={}",
                r.getStatusCode(), r.getBody().get("code").asText());
        assertEquals(429, r.getStatusCode().value());
        assertEquals("BACKLOG_LIMIT_REACHED", r.getBody().get("code").asText());

        // 批量规模上限 50：构造 51 条（在积压限额之外先被 BATCH_TOO_LARGE 拦截）
        List<Map.Entry<Long, ObjectNode>> big = new ArrayList<>();
        for (long o = 1; o <= 51; o++) {
            big.add(item(o, record("big-" + o, "n", o)));
        }
        ResponseEntity<JsonNode> r2 = push(sid, token, batch(big));
        log.info("[INPUT] batch size 51 -> status={}, code={}",
                r2.getStatusCode(), r2.getBody().get("code").asText());
        assertEquals(400, r2.getStatusCode().value());
        assertEquals("BATCH_TOO_LARGE", r2.getBody().get("code").asText());

        // 被拒绝批次不留任何痕迹
        JsonNode offset = getAdmin("/admin/sources/" + sid + "/offset");
        JsonNode data = getAdmin("/admin/sources/" + sid + "/data");
        JsonNode metrics = getAdmin("/admin/sources/" + sid + "/metrics");
        log.info("[STATE] after rejects offset={}, rows={}, rejectedCount={}",
                offset.get("offset").asLong(), data.size(), metrics.get("rejected").asLong());
        assertEquals(0, offset.get("offset").asLong());
        assertEquals(0, data.size());
        assertTrue(metrics.get("rejected").asLong() >= 2, "拒绝数量必须可观测");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
